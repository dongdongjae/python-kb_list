postgresql db를 만들어서 exl 파일의 각 시티를 table로 만들고 map table을 이용하여 list 테이블 만들기

### 파일구조
```
├── xlsx
│   └── kb_v2.1.0_map_unlock.xlsx
│
├── create_table.py
├── db_params.py
└── main.py
```
</br>

- create_table.py : PostgreSQL db에 xlsx 각 시트를 테이블로 생성

- db_params.py : PostgreSQL 연결 정보

- main.py : v2_map 테이블을 가지고 kb_v2_list 테이블 생성