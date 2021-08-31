from fastapi import FastAPI
import cx_Oracle
from typing import Optional
from fuzzywuzzy import fuzz


cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_12")


def search_name_in_black_list(received_second_name, received_first_name, received_middle_name):
    connection = cx_Oracle.connect(user="*****", password="****", dsn="*******")
    cursor = connection.cursor()
    cursor.execute("""select DISTINCT second_name, first_name, middle_name, 
                second_name_tran, first_name_tran, middle_name_tran, dob from reporter.black_list""")
    black_list = cursor.fetchall()
    matched_names = []

    received_fio = f"{received_second_name} {received_first_name} {received_middle_name}"
    received_fi = f"{received_second_name} {received_first_name}"

    for row in black_list:
        second_name = row[0]
        first_name = row[1]
        middle_name = row[2]

        second_name_eng = row[3]
        first_name_eng = row[4]
        middle_name_eng = row[5]

        dob = row[6]

        fio_rus = f"{second_name} {first_name} {middle_name}"
        fi_rus = f"{second_name} {first_name}"
        fio_eng = f"{second_name_eng} {first_name_eng} {middle_name_eng}"
        fi_eng = f"{second_name_eng} {first_name_eng}"

        matched_name_info = f"{fio_rus}, год рождения: {dob}"
        if fuzz.token_sort_ratio(received_fio, fio_rus) >= 90:
            matched_names.append(matched_name_info)
        elif fuzz.token_sort_ratio(fi_rus, received_fi) >= 90:
            matched_names.append(matched_name_info)
        elif fuzz.token_sort_ratio(received_fio, fio_eng) >= 90:
            matched_names.append(matched_name_info)
        elif fuzz.token_sort_ratio(fi_eng, received_fi) >= 90:
            matched_names.append(matched_name_info)

    cursor.close()
    connection.close()
    return matched_names


app = FastAPI()


@app.get("/get_matched_names/")
async def get_matched_names(second_name: str, first_name: str,
                            page: int, count: Optional[int] = 10, middle_name: Optional[str] = None):
    result = search_name_in_black_list(second_name, first_name, middle_name)
    return {
        "matched_name": result[(page-1) * count:page * count],
        "total": len(result)
    }
