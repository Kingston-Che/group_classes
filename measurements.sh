#!/bin/bash
#################################################
# Project: Expand the JSON data long format
# Branch: 
# Author: Kingston, the Data Engineer
# Created: 2025-01-14
# Updated: 2025-01-14
# Note: warehouse_production.measurements -> cofit_data_warehouse.group_class_logs
#################################################

## 取得登入資訊
base_dir=`pwd`
source $base_dir/postgres.secret

## 設立登入 function
run_on_prod() {
    psql "host=$host port=$port user=$puser password=$ppwd dbname=$db_prod sslcert=$SSL_CERT sslkey=$SSL_KEY" -t -c "$1"
}
run_on_cofi() {
    psql "host=$host port=$port user=$puser password=$ppwd dbname=$db_cofi sslcert=$SSL_CERT sslkey=$SSL_KEY" -t -c "$1"
}


## $2 = group_class.id
## $2 = measurements.id

## 清空暫存檔案
rm -rf tmp.txt data.txt input.sql

## 先拿到第一部分固定資料的寫入指令：
sql_class="
    SELECT 
        concat('(', 
            concat_ws(', ', 
                quote_literal(gc.program_id), 
                quote_literal(gc.id), 
                quote_literal(gc.started_at), 
                quote_literal(gc.finished_at), 
                quote_literal(gc.user_id),
                quote_literal(me.member_id), 
                quote_literal(extract(week from me.date)), 
                quote_literal(date_part('day', me.date::timestamp - gc.started_at::timestamp) + 1),
                quote_literal(me.date), 
                quote_literal(
                    CASE me.provider
                        WHEN 'fora'                 THEN  0
                        WHEN 'omron'                THEN  1
                        WHEN 'InBody'               THEN  2
                        WHEN 'FileUpload'           THEN  3
                        WHEN 'AbdominalUltrasound'  THEN  4
                        WHEN 'soft_bio'             THEN  5
                        WHEN 'imedtac'              THEN  6
                        WHEN 'TG3D'                 THEN  7
                        WHEN 'app'                  THEN  8
                        WHEN 'pro_web'              THEN  9
                        WHEN 'accu'                 THEN 10
                        WHEN 'phalanx'              THEN 11
                        WHEN 'tanita'               THEN 12
                        WHEN 'manual'               THEN 13
                        ELSE -1
                    END),
                quote_literal(me.id)   
                ), 
        ',')
    FROM group_classes gc, 
        group_class_orders gco, 
        measurements me
    WHERE 1 = 1
        AND gc.id = gco.group_class_id
        AND gco.client_id = me.member_id
        AND gc.id = $1
        AND me.id = $2
    ;"
# echo $sql_class
run_on_prod "$sql_class" >> tmp.txt

## 由於 Postgres 不接受空值寫入，故整理空值的內容為 NULL
sed -i "s/^,$/NULL,/g" tmp.txt


## 取得 data 欄位內的資料，另存入 data.txt
run_on_prod "SELECT data FROM measurements WHERE id = $2;" -t | jq > data.txt
## echo 
## 移除帶有 { 與 } 的兩行，剛好就是首末兩行
sed -i '1d' data.txt
sed -i '$d' data.txt
# cat data.txt

## 利用迴圈，逐一讀取 JSON 內容
while read -r line; 
do 
    #echo $line
    ## Postgres 不接受 INSERT 時的字串用雙引號包裹，故要拿掉
    item=`echo $line | cut -d : -f 1 | tr -d \"`
    value=`echo $line | cut -d : -f 2 | sed 's/,$//g' | tr -d \" | sed 's/^[[:blank:]]*//;s/[[:blank:]]*$//'`
    # echo item = $item
    # echo value = $value
    # cp tmp.txt input.sql

    ## 建立寫入用 SQL 檔案，以下為格式調整
    echo >> input.sql
    cat tmp.txt >> input.sql
    echo "'$item'", >> input.sql
    echo "'$value'" >> input.sql
    echo '),' >> input.sql
done < data.txt

#cat input.sql
## 修正 SQL 檔的第一行與最後一行
sed -i '$d' input.sql
echo ');' >> input.sql
## 補上 INSERT INTO 的完整指令到 input.sql
sed -i '1i INSERT INTO group_class_logs (program_id, class_id, started_at, finished_at, user_id, client_id, week, which_day,date, source, by_measurements, key, value) VALUES' input.sql
# cat input.sql
## 執行 SQL 檔案的執行
psql "host=$host port=$port user=$puser password=$ppwd dbname=$db_cofi sslcert=$SSL_CERT sslkey=$SSL_KEY" -f input.sql

## 清空暫存檔案
rm -rf tmp.txt data.txt input.sql
