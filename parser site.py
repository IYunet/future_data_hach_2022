import time

from bs4 import BeautifulSoup
import requests
import re
import pandas as pd

def get_clear_str(string):
    return " ".join(re.findall("\w+", string))

my_headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OSX 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",  "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"}

session = requests.Session()
base_url = "https://tgstat.ru"
url = base_url + "/ratings/channels/blogs/"
r = session.get(url, headers=my_headers, )
soup = BeautifulSoup(r.text, 'html.parser')
ratings_col = soup.find(id="sticky-left-column__inner")
links = ratings_col.findAll("a")
ratings = [link.get("href") for link in links if link.get("href") and link.get("href").startswith("/ratings")][1:]
data = []
t_rating = 1
for rating in ratings:
    try:
        url = base_url + rating
        url_t = url.split("?")
        topic = url_t[0].split("/")[-1]
        url_t[0] += "?sort=reach"
        url = url_t[0]
        r = session.get(url, headers=my_headers, )
        soup = BeautifulSoup(r.text, 'html.parser')
        channels_list = soup.find(id="sticky-center-column")
        users_links = [a.get("href") for a in channels_list.findAll("a")]
        t_user = 1
        for user_link in users_links:
            try:
                row = {}

                url = user_link
                r = session.get(url, headers=my_headers, )
                soup = BeautifulSoup(r.text, 'html.parser')

                name = soup.find("h1", class_="text-dark text-center text-sm-left").get_text()
                name = get_clear_str(name)


                profile_link = soup.find("a", class_="btn btn-outline-info btn-rounded px-3 py-05 font-14 text-truncate")
                tg_link = profile_link.get("href")
                username = get_clear_str(profile_link.get_text())

                row["name"] = name
                row["username"] = username
                row["tg_link"] = tg_link
                row["category"] = topic

                stats = soup.find(id="sticky-center-column").findAll("div", class_="col-lg-6 col-md-12 col-sm-12")
                if username=="приватный канал":
                    valid_card = [0,2,3,4,5,6]
                    for e,stat in enumerate(stats):
                        if e in valid_card:
                            value = get_clear_str(stat.find("h2").get_text())
                            title = get_clear_str(
                                stat.find("div", class_="position-absolute text-uppercase text-dark font-12").get_text())
                            if title == "возраст канала":
                                title = "дата создания"
                                value = stat.find("b", class_="text-dark mr-2 font-20").get_text()
                            row[title] = value
                else:
                    for stat in stats:
                        value = get_clear_str(stat.find("h2").get_text())
                        title = get_clear_str(stat.find("div", class_="position-absolute text-uppercase text-dark font-12").get_text())
                        if title == "возраст канала":
                            title = "дата создания"
                            value = stat.find("b", class_="text-dark mr-2 font-20").get_text()
                        row[title] = value

                data.append(row)
            except Exception as e:
                print(str(e))
                print("skip")
                continue
            finally:
                print(f"{t_user} юзеров из {len(users_links)} || {t_rating} категорий из {len(ratings)}")
                t_user += 1
    except Exception as e:
        print(e.message)
    finally:
        t_rating += 1
        continue


pd.DataFrame(data).to_csv("users.csv")