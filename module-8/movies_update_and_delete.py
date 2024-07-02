import mysql.connector
from mysql.connector import errorcode

config = {
    "user": "movies_user",
    "password": "popcorn",
    "host": "127.0.0.1",
    "database": "movies",
    "raise_on_warnings": True

}

def show_films (cursor,title):
   
    cursor.execute("select film_name as Name, film_director as Director, genre_name as Genre, studio_name as 'Studio Name' "
                   "FROM film INNER JOIN genre ON film.genre_id = genre.genre_id INNER JOIN studio ON film.studio_id = studio.studio_id ORDER BY film.film_id")
    films = cursor.fetchall()
    print("\n -- {} --".format(title))
    for directors in films:
        print("Film Name: {}\n Director: {}\n Genre Name ID: {}\n Studio Name: {}\n".format(directors[0],directors[1],directors[2],directors[3]))
    


try:
    db =mysql.connector.connect(**config)

    print("\n Database user {} connected to MySQL on {} with database {}".format(config["user"], config["host"], config["database"]))

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
    show_films(cursor, " DISPLAYING FILMS")


    cursor.execute("INSERT INTO film  VALUES (4,'Saw','2020','120',' Jordan Peele',3,3)")
    show_films(cursor," DISPLAYING FILMS AFTER INSERT")
    
   
       
    #update   
    cursor.execute("UPDATE film SET genre_id = 3 WHERE film_name = 'Alien'")
    show_films(cursor," DISPLAYING FILMS AFTER UPDATE- Changed Alien to Horror " )


    #Delete
    cursor.execute("DELETE FROM film WHERE film_name = 'Gladiator'")
    show_films(cursor," DISPLAYING FILMS AFTER DELETE ")
     
    db.close()