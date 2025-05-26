import requests #fazer requisições http
from bs4 import BeautifulSoup #parsear o html da pagina
import psycopg2 # conectar ao postgre
from datetime import datetime #manipulação de datas
from telegram import Bot #enviar mensagens no telegram
import asyncio # trabalhar com funções assíncronas 
from telegram.error import RetryAfter #usado para tratar limites do telegram
from dotenv import load_dotenv # ler variáveis de ambiente
import os # permite que o python acesse recursos do sistema

load_dotenv()

#credencias para enviar mensagens no telegram
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')                           
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

#inicia o bot criado no telegram com o token
bot = Bot(token=TELEGRAM_TOKEN)

#credenciais de conexão com o postgre
pg_conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
)
pg_cursor = pg_conn.cursor() # 'ponte' de comunicação entre o python e o postgre

#criação da tabela para armazenar os produtos extraídos, caso a tabela ainda não exista
pg_cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        product_name TEXT UNIQUE,
        old_price NUMERIC,
        new_price NUMERIC,
        discount NUMERIC,
        link TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        sent_telegram BOOLEAN DEFAULT FALSE
    );
''')
pg_conn.commit() #comitando para garantir que a tabela seja criada antes de qualquer coisa

#função para enviar mensagem para um chat no telegram
async def send_telegram_message(message):
    await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode="HTML")

def fetch_page(page_number):
    """
    Fazer requisição HTTP para a url do mercado livre, passando o numero da pagina
    que deseja, caso o status_code seja 200, retorna o html em texto,
    caso seja outro valor printa uma mensagem de erro.
    """
    url = f'https://www.mercadolivre.com.br/ofertas?page={page_number}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print('Erro na requisição.')

def parse_page(html):
    """
    Extrai os dados de cada produto da pagina utilizando
    seletores de classes CSS e cria uma lista de dicionario, 
    onde cada dicionario é um produto diferente.
    """
    soup = BeautifulSoup(html, 'html.parser') #faz o parsing do HTML
    search_result = soup.find_all('div', class_='andes-card') #procura todas as div com a classe informada
    products = []
    #vai percorrer produto por produto e extrair as informações(sempre verifica se o texto extraido existe)
    for result in search_result:
        product_name_tag = result.find('a', class_='poly-component__title')
        product_name = product_name_tag.text.strip() if product_name_tag else None
        fraction_tag = result.find('span', class_='andes-money-amount__fraction')
        cents_tag = result.find('span', class_='andes-money-amount__cents')
        if fraction_tag and cents_tag:
            old_price = f"{fraction_tag.text.strip()},{cents_tag.text.strip()}"
        elif fraction_tag:
            old_price = fraction_tag.text.strip()
        else:
            old_price = None
        new_price_tag = result.find('span', class_='andes-money-amount andes-money-amount--cents-superscript')
        new_price = new_price_tag.text.strip() if new_price_tag else None
        discount = None
        link_tag = result.find("a", class_="poly-component__title")
        link = link_tag['href'] if link_tag else None
        last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # se o produto possuir um titulo, vai criar um dicionario com os valores do produto e adicionar em uma lista
        if product_name:
            product = {
                'product_name': product_name,
                'old_price': old_price,
                'new_price': new_price,
                'discount': discount,
                'link': link,
                'last_updated': last_updated
            }
            products.append(product)
    return products

def clean_products(products):
    """
    Recebe os produtos para fazer o tratamento
    e faz o tratamento necessário, retirnado '.' e 'R$'
    para poder converter os numeros de preço e desconto para float, com duas casas decimais.
    """
    products_cleaned = []
    for product in products:
        old_price = product['old_price']
        new_price = product['new_price']
        discount = product['discount']

        if old_price:
            old_price = float(old_price.replace('.', '').replace(',', '.').replace('R$', ''))
            old_price = round(old_price, 2)
        if new_price:
            new_price = float(new_price.replace('.', '').replace(',', '.').replace('R$', ''))
            new_price = round(new_price, 2)
        if old_price and new_price:
            discount = round(((old_price - new_price) / old_price) * 100, 2)

        product_cleaned = {
            'product_name': product['product_name'],
            'old_price': old_price,
            'new_price': new_price,
            'discount': discount,
            'link': product['link'],
            'last_updated': product['last_updated']
        }
        products_cleaned.append(product_cleaned)

    return products_cleaned

def insert_products(products_cleaned):
    """
    Recebe os produtos tratados e insere os novos produtos na tabela products.
    Caso o product_name já existir(colua unique), ele atualiza os preços e as outras colunas,
    quando um produto já existente sofre apenas alteração no new_price, old_price ou discount,
    ele altera a coluan sent_telegram para false, fazendo que posteriormente envie o produto
    com o valor atualizado.
    """
    for product in products_cleaned:
        pg_cursor.execute("""
            INSERT INTO products (product_name, old_price, new_price, discount, link, last_updated, sent_telegram)
            VALUES (%s, %s, %s, %s, %s, %s, FALSE)
            ON CONFLICT (product_name) DO UPDATE SET
                old_price = EXCLUDED.old_price,
                new_price = EXCLUDED.new_price,
                discount = EXCLUDED.discount,
                link = EXCLUDED.link,
                last_updated = EXCLUDED.last_updated,
                sent_telegram = CASE 
                    WHEN ROUND(products.new_price::numeric, 2) IS DISTINCT FROM ROUND(EXCLUDED.new_price::numeric, 2)
                      OR ROUND(products.old_price::numeric, 2) IS DISTINCT FROM ROUND(EXCLUDED.old_price::numeric, 2)
                      OR ROUND(products.discount::numeric, 2) IS DISTINCT FROM ROUND(EXCLUDED.discount::numeric, 2)
                    THEN FALSE
                    ELSE products.sent_telegram 
                END
        """, (
            product['product_name'],
            product['old_price'],
            product['new_price'],
            product['discount'],
            product['link'],
            product['last_updated']
        ))
    pg_conn.commit()

async def send_unsent_telegram_messages():
    """
    Faz um select buscando todos os produtos que possuem o sent_telegram = false,
    armazena a mensagem formatada em uma variável, envia e da um update na coluna
    sent_telegram = true do produto que foi enviado, aguarda 8 segundos e envia o próximo
    produto. Caso ocorra o erro RetryAfter(que estava acontecendo) ele aguarda 8 segundos
    e tenta novamente.
    """
    pg_cursor.execute("SELECT id, product_name, old_price, new_price, discount, link FROM products WHERE sent_telegram = FALSE")
    rows = pg_cursor.fetchall()
    for row in rows:
        id_, product_name, old_price, new_price, discount, link = row

        message = (
            f"<b>{product_name}</b>\n"
            f"De: R$ {old_price:.2f}\n"
            f"Por: R$ {new_price:.2f}\n"
            f"Desconto: {discount}%\n"
            f'<a href="{link}">Link do produto</a>'
        )
        try:
            await send_telegram_message(message)
            pg_cursor.execute("UPDATE products SET sent_telegram = TRUE WHERE id = %s", (id_,))
            pg_conn.commit()
            # Espera 20 segundos antes de enviar a próxima mensagem
            await asyncio.sleep(8)
        except RetryAfter as e:
            print(f"Flood control: esperando {e.retry_after} segundos...")
            await asyncio.sleep(e.retry_after)
            # Após esperar, tenta enviar novamente
            await send_telegram_message(message)
            pg_cursor.execute("UPDATE products SET sent_telegram = TRUE WHERE id = %s", (id_,))
            pg_conn.commit()
            await asyncio.sleep(8)

async def main():
    """
    Função principal, onde percorre as paginas de 1 a 20 (todas da pagina de ofertas do ML)
    e chama as outras funções que foram definidas anteriormente, seguindo a sequencia: Percorre as paginas
    fazendo requisições http, extrai os dados de cada produto da pagina, limpa os dados e adicona em uma lista,
    após adicionados na lista faz a inserção no banco e posteriormente envia a mensagem no telegram. 
    """
    all_products = []
    for page in range(1, 21):
        html = fetch_page(page)
        if html:
            products = parse_page(html)
            products_cleaned = clean_products(products)
            all_products.extend(products_cleaned)

    insert_products(all_products)
    await send_unsent_telegram_messages()
    pg_cursor.close()
    pg_conn.close()

if __name__ == '__main__':
    """
    Roda a função principal assíncrona com asyncio.run(usado para iniciar o event loop do python)
    """
    asyncio.run(main())