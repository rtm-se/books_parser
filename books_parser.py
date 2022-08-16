import requests
import psycopg2
from bs4 import BeautifulSoup
# Using local credentials file instead of environmental variables for demo purposes
import credentials


def make_a_request(url) -> dict:

    response = requests.get(url)
    print(response.status_code)
    soup = BeautifulSoup(response.text, 'html.parser')
    next_data: dict = None
    # If we can find next button, copy it's url and make a recursive call on it.
    if soup.find('li', class_='next'):
        next_url = f'https://books.toscrape.com/{soup.find("li", class_="next").find("a").get("href")}'
        next_data = make_a_request(next_url)
    books_data = soup.findAll('li', class_='col-xs-6 col-sm-4 col-md-3 col-lg-3')
    # Replace() called to clean up the strings for late sql use.
    books_data = {
        book.find('img', class_='thumbnail').get('alt').replace("'", "''"):
            float(book.find('p', class_='price_color').text[2:])
        for book in books_data}
    # Updating the main data with the data from the recursive call
    if next_data:
        books_data.update(next_data)
    return books_data


def write_data(books_data) -> None:
    crsr = None
    try:
        with psycopg2.connect(
                host=credentials.HOSTNAME,
                dbname=credentials.DATABASE,
                user=credentials.USERNAME,
                password=credentials.PWD,
                port=credentials.PORT
        ) as connection:
            crsr = connection.cursor()

            table_sql = """
            CREATE TABLE IF NOT EXISTS book(
                book_id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
                title VARCHAR NOT NULL,
                price DECIMAL(5, 2),
                
                CONSTRAINT PK_book_book_id PRIMARY KEY(book_id)
            );
            """

            crsr.execute(table_sql)
            data_sql = """
            INSERT INTO book (title, price)
            VALUES 
            """
            strings: list[str] = []
            for title, price in books_data.items():
                strings.append(f"('{title}', {price})")
            data_sql = data_sql + ','.join(strings)

            crsr.execute(data_sql)
            connection.commit()
    except psycopg2.OperationalError:
        print('invalid credentials for psql')
    finally:
        if crsr:
            crsr.close()


def main() -> None:
    books_data = make_a_request('https://books.toscrape.com/')
    write_data(books_data)


if __name__ == '__main__':
    main()
