import requests
import sqlite3

from lxml.html import HtmlElement
from requests import Session
from lxml import html, etree

from src.command_line import get_user_credentials


def init_database(dbname: str = None):
    """
    Function that creates database and tables
    (Custom db for siriust.ru website)

    Parameters:
        dbname: str
            you can specialize name for database
    :return:
    """

    connection = sqlite3.connect(dbname + '.sqlite')
    connection_cursor = connection.cursor()

    tables = ''' 
    CREATE TABLE personal_info (
        id integer not null constraint personal_info_pk primary key autoincrement,
        user_email text not null, 
        firstname text not null, 
        lastname text not null,
        city text
        );
        
        create unique index personal_info_id_uindex on personal_info (id);
    
    CREATE TABLE favorite_products (
        id integer not null constraint favorite_products_pk primary key autoincrement,
        user_id integer not null constraint favorite_products_personal_info_id_fk
            references personal_info
            on delete cascade,
        product_name text not null,
        rating_value text,
        retail_price text,
        total_reviews int default 0,
        total_in_stock int
        );

        create unique index favorite_products_id_uindex on favorite_products (id);
    
    CREATE TABLE reviews (
        id integer not null constraint reviews_pk primary key autoincrement,
        product_id integer not null constraint reviews_favorite_products_id_fk
            references favorite_products
            on delete cascade,
        rating_value text not null,
        content text,
        reviewer text,
        published text);
    '''
    connection_cursor.executescript(tables)
    connection.close()


def insert_user_data(data: dict):
    """
    Function for execute INSERT statement into personal_info
    user table

    Parameters:
        data: dict
            Data to insert into personal info table
    Returns:
        None
    """
    with sqlite3.connect('siriust_db.sqlite') as conn:
        cursor = conn.cursor()
        try:
            params = data.get('user_email'), data.get('firstname'), data.get('lastname'), data.get('city'),
            cursor.execute(
                f"INSERT INTO personal_info (user_email, firstname, lastname, city) VALUES (?, ?, ?, ?);",
                params
            )
            conn.commit()
            return cursor.lastrowid
        except sqlite3.Error as err:
            print(err)
            conn.rollback()


def insert_favorite_products(data: list, last_row_id: int):
    """
    Function for execute INSERT statement into favorite_products and
    reviews

    Parameters:
        data: list
            Data with product info and all reviews data
        last_row_id: int
            Id for reviews table foreign key field
    Returns:
        None
    """

    for product in data:
        conn = sqlite3.connect('siriust_db.sqlite')
        cursor = conn.cursor()
        try:
            params = tuple(
                [last_row_id,
                 product.get('name'),
                 product.get('ratingValue'),
                 product.get('price'),
                 product.get('in_stock')]
            )
            cursor.execute(
                f"INSERT INTO favorite_products ("
                f"user_id, product_name, rating, retail_price, total_in_stock)"
                f"VALUES (?, ?, ?, ?, ?);",
                params
            )
            conn.commit()
            product_last_row = cursor.lastrowid
            if product.get('reviews'):
                for r in product.get('reviews'):
                    params_2 = tuple(
                        [product_last_row,
                         r.get('ratingValue'),
                         r.get('itemReviewed'),
                         r.get('name'),
                         r.get('datePublished')]
                    )

                    cursor.execute(
                        f"INSERT INTO reviews ("
                        f"product_id, rating_value, content, reviewer, published)"
                        f"VALUES (?, ?, ?, ?, ?);",
                        params_2
                    )
            conn.commit()
            conn.close()
        except sqlite3.Error as err:
            print(err)
            conn.rollback()
            conn.close()
            

def init_user_session():

    """
    Returns requests Session object with proper headers

    """

    # init_database()
    session = requests.Session()
    session.headers = {
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://siriust.ru',
        'referer': 'https://siriust.ru/',
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
    }
    return session


def get_authorized_session(url: str, credentials: tuple) -> Session:

    """
    Send request with user credentials

    Parameters:
        url: str
        credentials: tuple
    Returns:
        Session object
    """

    session = init_user_session()
    input_fields = get_form_input(session, url)
    data = prepare_request_payload(input_fields, credentials)
    r = session.post('https://siriust.ru', data=data)
    r.raise_for_status()
    return session


def get_login_form(fragment: HtmlElement, keywords: tuple) -> list:
    """
    Parameters:
         fragment: HtmlElement
             fragment from response string
         keywords: tuple
             given keywords for login form searching in DOM
    Returns:
         list with HtmlElement `form`
    """
    for k_word in keywords:
        form = fragment.xpath(
            f"//form[contains(@id, '{k_word}')] | "
            f"//form[contains(@name, '{k_word}')] | "
            f"//form[contains(@class, '{k_word}')]"
        )
        if form:
            return form


def get_form_input(session: Session, url: str) -> list | None:
    """
    Get list of all login form input fields

    Parameters:
        session: Session
        url: str
    Returns:
         iterable contains all inputs from login form or None
    Raises:
        requests.HttpError
    """
    r = session.get(url + '/login')
    r.raise_for_status()
    body = html.fragment_fromstring(
        r.text, create_parent='body')
    login_form = get_login_form(
        body, ('login', 'auth', 'signup'))
    input_fields = recursion_bypass(
        login_form, tags=('input', 'button'))
    return input_fields


def prepare_request_payload(iterable: list, credentials: tuple) -> dict:
    """
    Creates request payload with user credentials

    Parameters:
        iterable : list
            iterable includes HtmlElement
        credentials : tuple
            User login and password

    Returns:
        Dictionary object with payload for the request

    """
    payload = {}
    for elem in iterable:
        name = elem.attrib.get('name') or ""
        value = elem.attrib.get('value') or ""
        if name:
            if 'login' in name:
                payload[name] = credentials[0]
            elif 'password' in name:
                payload[name] = credentials[1]
            else:
                payload[name] = value
        else:
            continue
    return payload


def parse_data(url: str):
    """
    Collect user data from 'siriust.ru' website

    Parameters:
        url: str
    Raises:
        requests.HttpError
    """
    wishlist_products = []
    try:
        credentials = get_user_credentials()
        session = get_authorized_session(url, credentials)
        personal_info = get_personal_info(url, session)
        last_row_id = insert_user_data(personal_info)
        product_links = get_all_product_titles(url, session)
        for url in product_links:
            wishlist_products.append(get_product_info(url, session))
        insert_favorite_products(wishlist_products, last_row_id)
        print(wishlist_products, personal_info, sep='\n')
        return wishlist_products, personal_info
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)


def get_personal_info(url: str, session: Session) -> dict:

    """
    Get email, firstname, lastname and city of the customer
    Parse 'siriust.ru' website

    Parameters:
        url: str
        session: Session
    Returns:
        dictionary with user data

    """

    acc_info = {}
    r = session.get(url + '/profiles-update/')
    r.raise_for_status()
    parser = html.HTMLParser()
    tree = etree.HTML(r.content, parser)
    div = tree.body.find('div', {'id': 'content_general'})

    acc_info['user_email'] = div.get_element_by_id('email').value
    acc_info['firstname'] = tree.xpath("//input[contains(@name, 'user_data[s_firstname]')]")[0].value
    acc_info['lastname'] = tree.xpath("//input[contains(@name, 'user_data[s_lastname]')]")[0].value
    acc_info['city'] = tree.xpath("//input[contains(@name, 'user_data[b_city]')]")[0].value
    return acc_info


def get_all_product_titles(url: str, session: Session) -> list | None:
    """
    Get all links to products in a wishlist

    Parameters:
        url: str
        session: Session

    Returns:
         list with all product titles or None
    """
    r = session.get(url + '/wishlist/')
    r.raise_for_status()
    tree = generate_tree(r.content)
    product_titles = tree.xpath("//a[contains(@class, 'product-title')]/@href")
    if not product_titles:
        return
    else:
        return product_titles


def get_product_info(url: str, session: Session) -> dict:
    """

    Get all wishlist product info

    Parameters:
        url: str
        session: Session

    Returns:
         list with all product titles or None
    """
    r = session.get(url)
    r.raise_for_status()
    tree = generate_tree(r.content)
    product_detail = tree.xpath("//div[contains(@itemtype, 'schema.org/Product')]")[0]
    meta_tags = recursion_bypass(
        product_detail.getchildren(),
        tags=('meta',)
    )
    product_info = {k.attrib.get('itemprop'): k.attrib.get('content') for k in meta_tags}
    product_info['in_stock'] = in_stock_count(tree)

    if not product_info.get('reviewCount'):
        product_info['reviews'] = 0
        product_info['ratingValue'] = 0
    else:
        reviews = get_all_reviews(tree)
        product_info['reviews'] = reviews

    return product_info


def in_stock_count(fragment: HtmlElement) -> int:
    if fragment.xpath("//span[contains(@class, 'out-of-stock')]"):
        return 0
    count = 0
    product_features = fragment.xpath(
        "//div[contains(@id, 'content_features')]/div/*")
    for div in product_features:
        value = ''.join([div.getchildren()[-1].text_content()])
        if 'отсутствует' in value:
            continue
        else:
            count += 1
    return count


def get_all_reviews(fragment: HtmlElement):
    container = []
    review_list = fragment.xpath("//div[contains(@id, 'posts_list_')]/div")[0]
    for span in review_list.xpath("//span[contains(@itemtype, '/Review')]"):
        meta_tags = recursion_bypass(
            span.getchildren()
        )
        post = {tag.attrib.get('itemprop'): tag.attrib.get('content') for tag in meta_tags}
        container.append(post)
    return container


def generate_tree(content: bytes) -> HtmlElement:
    parser = html.HTMLParser()
    return etree.HTML(content, parser)


def recursion_bypass(iterable: list, tags: tuple = None, i: int = 0, ) -> list:
    container = []
    while i < len(iterable):
        if bool(iterable[i].getchildren()):
            container += recursion_bypass(
                iterable=iterable[i].getchildren(),
                tags=tags)
        else:
            if tags:
                if iterable[i].tag in tags:
                    container += [iterable[i]]
            else:
                container += [iterable[i]]
        i += 1
    else:
        return container



