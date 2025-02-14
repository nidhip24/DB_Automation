# Import zipfile to handle zip files
import zipfile

# Import pymysql to interact with MySQL database
import pymysql

# Import sys module
import sys

# Establish the database connection using pymysql
connection = pymysql.connect(
    host='mysql-a2-nk.mysql.database.azure.com',
    user='dbadmin',
    password='nidhip1234@',
    database= "companydb"
)

def run_cmds(cursor, file):
    sql_script = file.read().decode('utf-8')
    lines = sql_script.split(";")

    for line in lines:
        if line.strip():
            cursor.execute(line)

def process_file(file_name, is_zip = False):
    try:
        cursor = connection.cursor()
        if (is_zip):
            with zipfile.ZipFile(file_name, 'r') as zf:
                zip_files = zf.namelist()
                zip_files.sort()
                for script in zip_files:
                    if (script.endswith("sql")):
                        with zf.open(script) as file:
                            print(f"processing {script} file")
                            run_cmds(cursor, file)
        else:
            run_cmds(cursor, open(file_name, 'rb'))
    finally:
        cursor.close()

def deploy_migration(file_name):
    print(f"Processing {file_name}")

    try:
        if (file_name.endswith("sql")):
            print("processing single file")
            process_file(file_name)
        elif (file_name.endswith("zip")):
            print("processing zip file")
            process_file(file_name, True)
        
        connection.commit()
        print(f"{file_name} processed successfully")
    except Exception as e:
        print(f"Exception raise while processing {file_name}: {str(e)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python deploy_changes.py [file_name]")
        sys.exit(1)
    
    action = sys.argv[1].lower()

    try:
        if action.strip():
            deploy_migration(action)
    finally:
        connection.close()