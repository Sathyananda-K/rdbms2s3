import csv
import time
import pyodbc


class ExecutionResult:
    def __init__(self, status):
        self.status = status


class Success(ExecutionResult):
    def __init__(self, row_count, time):
        super().__init__(True)
        self.row_count = row_count
        self.time = time


class Failure(ExecutionResult):
    def __init__(self, error):
        super().__init__(False)
        self.error = error


def query_to_csv(conn, sql, output):
    try:
        start_time = time.time()
        cursor = conn.cursor()
        cursor.arraysize = 51200
        rows_count = 0
        cursor.execute(sql)
        header = [x[0] for x in cursor.description]
        while True:
            rows = cursor.fetchmany()
            if not rows:
                break
            rows_count = rows_count + rows_to_csv(header, rows, output)
        return Success(rows_count, time.time() - start_time)
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        return Failure(sqlstate)


def rows_to_csv(column_names, rows, output):
    writer = csv.writer(output)
    # write header
    writer.writerow(column_names)
    # write rows
    count = 0
    for row in rows:
        writer.writerow(row)
        count = count + 1

    return count