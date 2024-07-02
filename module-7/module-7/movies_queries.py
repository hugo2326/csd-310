import mysql.connector
from mysql.connector import errorcode

config = {
    "user": "movies_user",
    "password": "popcorn",
    "host": "127.0.0.1",
    "database": "movies",
    "raise_on_warnings": True

}

try:
    db = mysql.connector.connect(**config)

    print("\n Database user {} connected to MySQL on {} with database {}".format(config["user"], config["host"],
                                                                                 config["database"]))

    input("\n\n Press any key to continue...")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print(" The supplied username or password are invalid")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print(" The specified database does not exist")

    else:
        print(err)

finally:
    cursor = db.cursor()
    cursor.execute(" SELECT * FROM studio")
    film = cursor.fetchall()
    print(f"-- DISPLAYING Studio RECORDS --")
    for studio in film:
        print("Studio ID: {}\n Studio Name:{}\n".format(studio[0], studio[1]))  # three fields

    cursor.execute("SELECT * FROM genre")
    records = cursor.fetchall()
    print(f"-- DISPLAYING Genre RECORDS --")
    for genre in records:
        print("Genre ID: {}\n Genre Name: {}\n".format(genre[0], genre[1]))

    cursor.execute("SELECT film_name,film_runtime FROM film WHERE film_runtime < 120")
    short_film = cursor.fetchall()
    print(f"-- DISPLAYING Short Film RECORDS --")
    for films in short_film:
        print("Film Name: {}\n Runtime: {}\n".format(films[0], films[1], ))

    cursor.execute("SELECT film_name,film_director FROM film ORDER BY film_director")
    director = cursor.fetchall()
    print(f"-- DISPLAYING Director RECORDS in Order --")
    for directors in director:
        print("Film Name: {}\n Director: {}\n".format(directors[0], directors[1]))

    db.close()