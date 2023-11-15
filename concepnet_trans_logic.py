import pandas as pd
import requests
import datetime
import mariadb
import time
import json
import os
from tqdm import tqdm
import urllib.request

# from kbutil.dbutil import db_connector

def db_connector(db_type):
    # Connect to MariaDB Platform
    conn = ""

    if db_type == "local":
        # 로컬 DB 접속정보
        print("# 로컬 DB 접속정보 ============================== ")
        conn = mariadb.connect(
            user="root",
            password="mariadb2022",
            host="127.0.0.1",
            port=3306,
            database="kbvqa"
        )

    return conn

if __name__ == '__main__':

    print("Concepnet DB 키워드 번역 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    # 파파고 API
    client_id = "client_id"                      # 개발자센터에서 발급받은 Client ID 값
    client_secret = "clinet_secret"              # 개발자센터에서 발급받은 Client Secret 값

    # DB 연결
    conn = db_connector("local")
    cur = conn.cursor()

    relation_list = ['RelatedTo', 'AtLocation', 'HasProperty', 'IsA', 'CapableOf', 'UsedFor',
                     'DerivedFrom', 'Desires', 'HasA', 'ReceivesAction', 'PartOf', 'CreatedBy']

    relation_dict = {
        'RelatedTo': '관계', 
        'AtLocation': '위치', 
        'HasProperty': '속성', 
        'IsA': '~이다', 
        'CapableOf': '~할 수 있다', 
        'UsedFor': '사용 용도',
        'DerivedFrom': '파생 항목', 
        'Desires': '욕망', 
        'HasA': '~가 있다', 
        'ReceivesAction': '행동을 받다', 
        'PartOf': '~의 일부', 
        'CreatedBy': '만들어지다'
    }
    
    dict_data = dict()

    # 전체 데이터 검색
    sql_select = """ SELECT id, big_class, small_class, search_word, word_en, e1_label, e2_label, r, use_yn, e1_label_translate, 
                            e2_label_translate, r_translate, visual_concept, modf_date 
                       FROM knowlegebase_db
                      WHERE collect_target = 'concepnet'
                        AND reg_date > '2022-12-05'
                 """
    cur.execute(sql_select)
    data_result = cur.fetchall()

    word_list = []
    
    for one in data_result:
        e1_label = one[5]
        e2_label = one[6]

        e1_label_trans = one[9] 
        e2_label_trans = one[10]

        if e1_label_trans == None or e1_label_trans == '':
            word_list.append(e1_label)

        if e2_label_trans == None or e2_label_trans == '':
            word_list.append(e2_label)

    word_set = set(word_list)
    word_set = list(word_set)
    word_set = word_set[::-1]
    
    url = "https://openapi.naver.com/v1/papago/n2mt"
    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id", client_id)
    request.add_header("X-Naver-Client-Secret", client_secret)

    for one_word in tqdm(word_set):
        # print()
        # print(one_word)

        sql_e1_word = """ SELECT id, big_class, small_class, search_word, word_en, e1_label, e2_label, r, use_yn, e1_label_translate, e2_label_translate, r_translate, modf_date 
                            FROM knowlegebase_db
                           WHERE collect_target = 'concepnet'
                             AND reg_date > '2022-12-05'
                             AND e1_label = ?
                             AND e1_label_translate IS NULL
                      """

        sql_e2_word = """ SELECT id, big_class, small_class, search_word, word_en, e1_label, e2_label, r, use_yn, e1_label_translate, e2_label_translate, r_translate, modf_date 
                            FROM knowlegebase_db
                           WHERE collect_target = 'concepnet'
                             AND reg_date > '2022-12-05'
                             AND e2_label = ?
                             AND e2_label_translate IS NULL
                      """

        word_values = (one_word, )

        cur.execute(sql_e1_word, word_values)
        e1_result = cur.fetchall()

        cur.execute(sql_e2_word, word_values)
        e2_result = cur.fetchall()
        
        # 번역 부분
        word_translated = ''
        
        word_enc_text = urllib.parse.quote(one_word)
        word_data = "source=en&target=ko&text=" + word_enc_text
        word_response = urllib.request.urlopen(request, data=word_data.encode("utf-8"))
        word_rescode = word_response.getcode()

        time.sleep(0.005)

        if(word_rescode == 200):
            word_response_body = word_response.read()
            word_response_body = word_response_body.decode('utf-8')
            word_response_body = json.loads(word_response_body)
            word_translated = word_response_body['message']['result']['translatedText']
            # print(e1_response_body.decode('utf-8'))
        else:
            print("e1 label >>> " + e1_label)
            print("Error Code:" + word_rescode)
        

        for o_1 in e1_result:
            kb_id = o_1[0]

            sql_update = """ UPDATE knowlegebase_db
                                SET e1_label_translate = ?,
                                    use_yn = 'N',
                                    modf_date = current_timestamp()
                              WHERE id = ?
                """
            update_values = (word_translated, kb_id)
            cur.execute(sql_update, update_values)
            conn.commit()
            time.sleep(0.00001)

        for o_2 in e2_result:            
            kb_id = o_2[0]

            sql_update = """ UPDATE knowlegebase_db
                                SET e2_label_translate = ?,
                                    use_yn = 'N',
                                    modf_date = current_timestamp()
                              WHERE id = ?
                """
            update_values = (word_translated, kb_id)
            cur.execute(sql_update, update_values)
            conn.commit()
            time.sleep(0.00001)

    conn.close()
    print('--- 번역 완료 (SUCCESS) ------------------------------------------------------------')
    print('--- END ---------------------------------------------------------------------------')
