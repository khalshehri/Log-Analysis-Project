#!/usr/bin/env python3
import psycopg2

DBNAME = "news"


def run_query(query):
    try:
        db = psycopg2.connect('dbname=' + DBNAME)
        c = db.cursor()
        c.execute(query)
        results = c.fetchall()
        db.close()
        return results
    except (psycopg2.DatabaseError, e):
        print("<error message>")


def top_articles():
    query = """
        SELECT title, COUNT(*) AS num
        FROM articles, log
        WHERE path LIKE concat('%', slug)
        GROUP BY title
        ORDER BY num DESC
        LIMIT 3;
    """
    results = run_query(query)

    print('\n1. What are the most popular three articles of all time?')
    count = 1
    for i in results:
        print(str(count) + ' - "' + i[0] + '"  : ' + str(i[1]) + " views")
        count += 1


def top_authors():
    query = """
        SELECT authors.name, COUNT(*) AS num
        FROM authors, articles, log
        WHERE authors.id = articles.author
        AND log.path like concat('%', articles.slug)
        GROUP BY authors.name
        ORDER BY num DESC
        LIMIT 4;
    """

    results = run_query(query)

    print('\nWho are the most popular article authors of all time?')
    count = 1
    for i in results:
        print(str(count) + '- ' + i[0] + ' : ' + str(i[1]) + " views")
        count += 1


def get_errors():
    query = """
        SELECT logs.day,
          ROUND(((errors.bad_requests*1.0) / logs.requests),4) AS percent
        FROM (
          SELECT date_trunc('day', time) "day", count(*) AS bad_requests
          FROM log WHERE status LIKE '40%' GROUP BY day ) AS errors
        JOIN (
          SELECT date_trunc('day', time) "day", count(*) AS requests
          FROM log GROUP BY day ) AS logs ON logs.day = errors.day
        WHERE (ROUND(((errors.bad_requests*1.0) / logs.requests),4) > 0.01)
        ORDER BY percent DESC LIMIT 1;
    """

    results = run_query(query)

    print('\nOn which days did more than 1% of requests lead to errors?')
    for i in results:
        date = i[0].strftime('%B %d, %Y')
        errors = str(round(i[1]*100, 2)) + "%" + " errors"
        print(date + " has " + errors)


if __name__ == '__main__':
    print(' Results ')
    top_articles()
    top_authors()
    get_errors()
