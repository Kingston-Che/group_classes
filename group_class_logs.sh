#!/bin/bash
#################################################
# Project: 把身體量測的資料，納入八週課程的框架中
# Branch: 
# Author: Kingston, the Data Engineer
# Created: 2025-01-14
# Updated: 2025-01-15
# Note: warehouse_production.client_entries -> cofit_data_warehouse.group_class_logs
#################################################
## 取得登入資訊
base_dir=`pwd`
source $base_dir/postgres.secret

#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y%m%d"`
fi


sql_create="
    CREATE TABLE IF NOT EXISTS group_class_logs (
        program_id integer NOT NULL, 
        class_id integer NOT NULL, 
        started_at date NOT NULL, 
        finished_at date NOT NULL, 
        key varchar(64) NOT NULL, 
        client_id integer NOT NULL, 
        week integer NOT NULL, 
        which_day integer NOT NULL, 
        date date NOT NULL, 
        value varchar(32) NOT NULL,
        source integer NOT NULL, 
        by_measurements bigint NOT NULL, 
        user_id integer NOT NULL
    );"
sql_idx="
    CREATE UNIQUE INDEX uni_group_class_logs ON public.group_class_logs 
        (program_id, class_id, started_at, finished_at, key, client_id, week, which_day, date, value, source, by_measurements, user_id);
    CREATE INDEX idx_group_class_logs_date ON public.group_class_logs USING btree (date);
    CREATE INDEX idx_group_class_logs_key ON public.group_class_logs USING btree (key);
    CREATE INDEX idx_group_class_logs_program ON public.group_class_logs USING btree (program_id);
    CREATE INDEX idx_group_class_logs_user ON public.group_class_logs USING btree (user_id);    
    "
sql_comment="
    COMMENT ON TABLE public.group_class_logs IS '課程或療程期間，學員所有的身體量測資料';
    COMMENT ON COLUMN public.group_class_logs.program_id IS '課程或療程的所屬方案（= program.id）';    
    COMMENT ON COLUMN public.group_class_logs.class_id IS '課程或療程的編號（= group_classes.id）';
    COMMENT ON COLUMN public.group_class_logs.started_at IS '課程或療程的起始日';
    COMMENT ON COLUMN public.group_class_logs.finished_at IS '課程或療程的結束日（不包含這天）';
    COMMENT ON COLUMN public.group_class_logs.key IS '身體量測指標';
    COMMENT ON COLUMN public.group_class_logs.client_id IS '課程或療程的參與學員之 ID 編號';
    COMMENT ON COLUMN public.group_class_logs.week IS '身體量測資料的當週週數';
    COMMENT ON COLUMN public.group_class_logs.which_day IS '身體量測資料位於課程的第 N 天';
    COMMENT ON COLUMN public.group_class_logs.date IS '身體量測資料的當天日期';
    COMMENT ON COLUMN public.group_class_logs.value IS '身體量測資料';
    COMMENT ON COLUMN public.group_class_logs.source IS '資料來源; -1:unknown, 0:fora, 1:omron, 2:inbody, 3:file_upload, 4:abdominal_ultrasound, 5:soft_bio, 6:imedtac, 7:tg3d, 8:app, 9:pro_web, 10:accu, 11:phalanx, 12:tanita, 13:manual';
    COMMENT ON COLUMN public.group_class_logs.by_measurements IS '>0: measurements 表中的 id; =0: 非來自 measurements 的資料'; 
    COMMENT ON COLUMN public.group_class_logs.user_id IS '課程或療程的帶班營養師之 ID 編號';    
    "
sql_grant="
    GRANT ALL ON public.group_class_logs TO bi_dod
    ;"


sql_being="
    SELECT EXISTS (
        SELECT 
        FROM 
            pg_tables
        WHERE 
            schemaname = 'public' AND 
            tablename  = 'group_class_logs'
        )
    ;"
being=`run_on_cofi "$sql_being" | sed 's/^[[:blank:]]*//;s/[[:blank:]]*$//'`

## 若資料表不存在則建立
if [[ "$being" == "f" ]]; 
then 
    run_on_cofi "$sql_create"
    run_on_cofi "$sql_idx"
    run_on_cofi "$sql_comment"
    run_on_cofi "$sql_grant"
fi



sql_class="
    SELECT id 
    FROM group_classes 
    WHERE kind in (0, 2) 
        AND aasm_state = 'finished' 
        AND finished_at = '$vDate'
    ORDER BY id
    ;"
classes=`run_on_prod "$sql_class"`
# echo class = $class


for class in $classes; 
do 
    ## 拿到參與課程的學員名單
    sql_clients="
        SELECT client_id
        FROM group_class_orders
        WHERE aasm_state = 'registered'
            AND group_class_id = $class
        ;"
    clients=`run_on_prod "$sql_clients"`

    for c in $clients; 
    do 
        #### 第一步：取得每個 key 在開課前的第一筆資料（列出開課前七後三的身體資料，拿到最接近開課日的資料）
        sql_to="
            SELECT 
                concat('(', 
                    concat_ws(', ', 
                        quote_literal(program_id), 
                        quote_literal(class_id), 
                        quote_literal(started_at), 
                        quote_literal(finished_at), 
                        quote_literal(key), 
                        quote_literal(client_id), 
                        quote_literal(week), 
                        quote_literal('0'), 
                        quote_literal(date), 
                        quote_literal(value),
                        quote_literal(coalesce(source, -1)), 
                        quote_literal(coalesce(measurement_id, 0)), 
                        quote_literal(user_id)   
                    ), 
                ');')
            FROM (
                SELECT 
                    gc.program_id, 
                    gc.id class_id, 
                    gc.started_at, 
                    gc.finished_at, 
                    ce.key, 
                    ce.client_id, 
                    ce.date,
                    abs(date_part('day', ce.date::timestamp - gc.started_at::timestamp)) daydiff,
                    extract(week from gc.started_at) - 1 week,
                    replace(coalesce(quote_literal(number_value), string_value), '''', '') value, 
                    ce.source,
                    ce.measurement_id,
                    gc.user_id,
                    ROW_NUMBER() OVER (PARTITION BY key ORDER BY abs(date_part('day', ce.date::timestamp - gc.started_at::timestamp)), ce.date, ce.created_at DESC, ce.updated_at DESC) AS row_number
                FROM group_classes gc
                    INNER JOIN group_class_orders gco
                        ON gc.id = gco.group_class_id
                    INNER JOIN client_entries ce
                        ON gco.client_id = ce.client_id
                WHERE 1 = 1
                    AND gc.id = 10871
                    AND gco.aasm_state = 'registered'
                    AND ce.date >= gc.started_at - interval '7' day 
                    AND ce.date <  gc.started_at + interval '3' day
                    AND coalesce(quote_literal(ce.number_value), ce.string_value) IS NOT NULL
                    AND ce.key NOT LIKE '%min'
                    AND ce.key NOT LIKE '%max'
                ) t
            WHERE row_number = 1
            ;"
        run_on_prod "$sql_to" | sed 's/^[[:blank:]]*//;s/[[:blank:]]*$//' > key_$c.txt
        # cat key_$c.txt
        # echo 

        #### 第二步：確認是否有人請假而延長結束日期與天數
        ## 計算邏輯：由於一人可多次請假，所以會把所有的請假天數加總；又由於請假是以週為單位，故只計算取 /7 所得之商數（Quotient），忽略 <7 的剩餘天數
        sql_leave="
            SELECT coalesce(7 * floor(sum(coalesce(date_part('day', finish_date::timestamp - start_date::timestamp), 0)) / 7), 0)
            FROM subscription_service_leaves 
            WHERE group_class_id = $class
                AND client_id = $c
            ;"
        leaves=`run_on_prod "$sql_leave" | sed 's/^[[:blank:]]*//;s/[[:blank:]]*$//'`

        ## 第三步：取得所有課程中的所有資料；包含有請假的人，其抓延後時間的資料
            sql_follow="
                SELECT 
                    concat('(', 
                        concat_ws(', ', 
                            quote_literal(program_id), 
                            quote_literal(class_id), 
                            quote_literal(started_at), 
                            quote_literal(finished_at), 
                            quote_literal(key), 
                            quote_literal(client_id), 
                            quote_literal(week), 
                            quote_literal(which_day), 
                            quote_literal(date), 
                            quote_literal(value),
                            quote_literal(coalesce(source, -1)), 
                            quote_literal(coalesce(measurement_id, 0)), 
                            quote_literal(user_id)   
                        ), 
                    ');')
                FROM (
                    SELECT 
                        gc.program_id, 
                        gc.id class_id, 
                        gc.started_at, 
                        gc.finished_at, 
                        ce.key, 
                        ce.client_id, 
                        ce.date,
                        extract(week from ce.date) week, 
                        date_part('day', ce.date::timestamp - gc.started_at::timestamp) + 1 which_day, 
                        replace(coalesce(quote_literal(number_value), string_value), '''', '') value, 
                        source, 
                        measurement_id,
                        user_id,
                        ROW_NUMBER() OVER (PARTITION BY ce.key, ce.date ORDER BY ce.created_at DESC, ce.updated_at DESC) AS row_number
                    FROM group_classes gc
                        INNER JOIN group_class_orders gco
                            ON gc.id = gco.group_class_id
                        INNER JOIN client_entries ce
                            ON gco.client_id = ce.client_id
                    WHERE 1 = 1
                        AND gc.id = $class
                        AND ce.client_id = $c
                        AND gco.aasm_state = 'registered'
                        AND ce.date >= gc.started_at
                        AND ce.date <  gc.finished_at + interval '$leaves' day
                        AND coalesce(quote_literal(ce.number_value), ce.string_value) IS NOT NULL
                        AND ce.key NOT LIKE '%min'
                        AND ce.key NOT LIKE '%max'
                    ) t
                WHERE row_number = 1
                ;"
            run_on_prod "$sql_follow" | sed 's/^[[:blank:]]*//;s/[[:blank:]]*$//' >> key_$c.txt
            # cat key_$c.txt

        #### 第四步：檢查是否有資料單獨在 measurements，有的話要進行資料寫入
        sql_measure="
            SELECT me.id
            FROM measurements me
                LEFT JOIN client_entries ce
                    ON me.date = ce.date
                        AND me.member_id = ce.client_id
                        AND me.id = ce.measurement_id
            WHERE me.date >= (SELECT started_at FROM group_classes WHERE id = $class)
                AND me.date < (SELECT finished_at FROM group_classes WHERE id = $class)	
                AND provider <> 'FileUpload'
                AND member_id = $c
                AND ce.measurement_id IS NULL
	        ;"
        measure=`run_on_prod "$sql_measure" | sed 's/^[[:blank:]]*//;s/[[:blank:]]*$//'`

        ## 若有 measurements.id 需要處理，則進行以下指令
        if [[ -n $measure ]]; 
        then 
            for m in $measure; 
            do 
                echo m = $measure
                ## measurements.sh 裡面的寫入格式需確認有一致
                bash $base_dir/measurements.sh $class $m 2>> group_class_logs.error
            done
        fi

        ## 清除空白行
        sed -i '/^$/d' key_$c.txt     

        #### 第五步：利用迴圈，逐一寫入資料
        while read -r line; 
        do 
            # echo $line
            ## 建立寫入用 SQL 檔案，以下為格式調整
            sql_insert="INSERT INTO group_class_logs VALUES $line" 
            run_on_cofi "$sql_insert" 2>> group_class_logs.error
            # echo $sql_insert
        done < key_$c.txt


        #### 第六步：獨立計算每人的 Sarcopenia Obesity(SO)
        ## 完整公式：so_score = round((left_arm_muscle+left_leg_muscle+right_arm_muscle+right_leg_muscle)*100/weight,2))
        sql_soscore="
            INSERT INTO group_class_logs
                SELECT *
                FROM (
                    SELECT 
                        program_id, 
                        class_id, 
                        started_at, 
                        finished_at, 
                        'so_score' key, 
                        client_id, 
                        week, 
                        date_part('day', date::timestamp - started_at::timestamp) + 1 which_day, 
                        date, 
                        round(100 * 
                            sum(
                                CASE WHEN key similar to '(left|right)%' AND key similar to '%(muscle)' THEN cast(value as DECIMAL) 
                                    ELSE NULL 
                                END) 
                                    / MAX(CASE WHEN key = 'weight' THEN cast(value as DECIMAL) ELSE NULL END
                                )
                            , 2) value, 
                        source, 
                        by_measurements, 
                        user_id
                    FROM group_class_logs 
                    WHERE class_id = $class
                        AND client_id = $c 
                    GROUP BY 
                        program_id, 
                        class_id, 
                        started_at, 
                        finished_at, 
                        client_id, 
                        week, 
                        date, 
                        source,
                        by_measurements, 
                        user_id
                    ) t
                WHERE value IS NOT NULL
            ;"
        # echo $sql_soscore
        run_on_cofi "$sql_soscore" 2>> group_class_logs.error

        rm -rf key_$c.txt
    done
done

