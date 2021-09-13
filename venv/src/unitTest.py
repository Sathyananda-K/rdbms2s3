from io import StringIO
import csv2console
import db2csv


def test():
    out = csv2console.ConsoleOutput()

    header = ["id", "name", "phone"]
    rows = [
        [1, "Nick", "800-123-2345"],
        [2, "Joe", "571-542-3352"]
    ]

    rows = db2csv.rows_to_csv(header, rows, out)
    print(rows)

def test1():
    out = StringIO()

    header = ["id", "name", "phone"]
    rows = [
        [1, "Nick", "800-123-2345"],
        [2, "Joe", "571-542-3352"]
    ]

    rows = db2csv.rows_to_csv(header, rows, out)
    print(out.getvalue())
    print(rows)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    test1()