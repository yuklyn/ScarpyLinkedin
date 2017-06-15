import sys
import time
import copy
import re
import urllib.parse
import requests
from lxml import etree
import json
import MySQLdb


LINKEDIN_URL_LOGIN = 'https://www.linkedin.com/uas/login'
LINKEDIN_RESEARCH_MAX_PAGE = 100
COMPANY_TABLE_LINK = 'Link'
COMPANY_TABLE_EMPLOYEE = 'Employee'

LINKED_COUNT = 0     # 已抓取的链接数
LINKS_FINISHED = []  # 已抓取的linkedin用户


def is_database_exists(db_conn, db_name):
    db_cursor = db_conn.cursor()
    db_cursor.execute("SHOW DATABASES")
    databases_tuple = db_cursor.fetchall()
    for database_tuple in databases_tuple:
        database_str = ''.join(database_tuple)
        if database_str.lower() == db_name.lower():
            db_cursor.close()
            return True
        db_cursor.close()
    return False


def is_table_exists(db_conn, table_name):
    db_cursor = db_conn.cursor()
    db_cursor.execute("SHOW TABLES")
    tables_tuple = db_cursor.fetchall()
    for table_tuple in tables_tuple:
        table_str = ''.join(table_tuple)
        if table_str.lower() == table_name.lower():
            db_cursor.close()
            return True
    db_cursor.close()
    return False


# 若数据库不存在，则创建；若表不存在，则创建Link、Employee两个表
def connect_database(host, port, user, password, database_name, company_name_en):
    try:
        db_conn = MySQLdb.connect(host=host,
                                  port=port,
                                  user=user,
                                  passwd=password,
                                  charset="utf8")
        db_cursor = db_conn.cursor()

        if not is_database_exists(db_conn, database_name):
            db_cursor.execute('CREATE DATABASE IF NOT EXISTS %s' % database_name)

        db_cursor.execute('USE %s' % database_name)

        if not is_table_exists(db_conn, company_name_en + COMPANY_TABLE_LINK):
            db_cursor.execute("CREATE TABLE %s(id int primary key auto_increment, "
                           "url varchar(100), "
                           "access varchar(20))" % (company_name_en + COMPANY_TABLE_LINK))
            db_cursor.execute("CREATE TABLE %s(id int primary key auto_increment, "
                           "firstName varchar(20), "
                           "lastName varchar(20), "
                           "school varchar(100))" % (company_name_en + COMPANY_TABLE_EMPLOYEE))
        return db_conn
    except MySQLdb.Error as mysql_error:
        print(mysql_error)
        return -1


def login(session_key, session_password):
    # Provides cookie persistence, connection-pooling, and configuration.
    s = requests.Session()
    r = s.get(LINKEDIN_URL_LOGIN)
    tree = etree.HTML(r.content)
    login_csrf_param = tree.xpath('//input[@id="loginCsrfParam-login"]/@value')
    source_alias = tree.xpath('//input[@id="sourceAlias-login"]/@value')
    is_js_enabled = tree.xpath('//input[@name="isJsEnabled"]/@value')

    payload = {
        'isJsEnabled': is_js_enabled,
        'session_key': session_key,
        'session_password': session_password,
        'loginCsrfParam': login_csrf_param,
        'sourceAlias': source_alias
    }

    response_login = s.post('https://www.linkedin.com/uas/login-submit', data=payload)
    # 检查是否需要输入验证码
    if response_login.url == 'https://www.linkedin.com/uas/consumer-email-challenge':
        tree = etree.HTML(response_login.content)
        sign_in = tree.xpath('//input[@id="btn-primary"]/@value')
        dts = tree.xpath('//input[@id="dts-ATOPinChallengeForm"]/@value')
        security_challenge_id = tree.xpath('//input[@id="security-challenge-id-ATOPinChallengeForm"]/@value')
        orig_source_alias = tree.xpath('//input[@id="origSourceAlias-ATOPinChallengeForm"]/@value')
        csrf_token = tree.xpath('//input[@id="csrfToken-ATOPinChallengeForm"]/@value')
        source_alias = tree.xpath('//input[@id="sourceAlias-ATOPinChallengeForm"]/@value')
        verification_code = input('请输入发送到邮箱或手机的验证码：')
        while(True):
            payloadV = {
                'PinVerificationForm_pinParam': verification_code,
                'signin': sign_in,
                'dts': dts,
                'security-challenge-id': security_challenge_id,
                'origSourceAlias': orig_source_alias,
                'sourceAlias': source_alias,
                'csrfToken': csrf_token
            }
            response_verification = s.post('https://www.linkedin.com/uas/ato-pin-challenge-submit', data=payloadV)
            if response_verification.url == 'https://www.linkedin.com/uas/ato-pin-challenge-submit':
                verification_code = input('请输入发送到邮箱的验证码，输入quit退出：')
                if verification_code.lower() == 'quit':
                    return -1
                continue
            return s
    # 不需要验证码
    if r.status_code != 200:
        print('登陆失败，错误码：%s' % r.status_code)
        return -1
    return s


# 返回一个列表，包含当前页面的全部员工主页连接（未查重）
def get_person_url_per_page(url, s):
    link_list_current_page = []

    try:
        r = s.get(url, timeout=20)
    except Exception as e:
        print(e)
        return -1

    print('正在抓取第%s页的员工主页链接' % url.split('=')[5])
    print('status: %s' % (r.status_code))
    content = urllib.parse.unquote(r.content.decode("utf-8"))
    content = content.replace('&quot;', '"')
    metadata = ''.join(re.findall('(\{"data":{"metadata":{"guides":.*?VerticalGuide"}]\})', content))
    person_info_list = re.findall('(\{[^\{]*?"firstName".*?"publicIdentifier".*?"\$type"[^\}]*?\})', metadata)
    for person_info in person_info_list:
        person_info_json = json.loads(person_info)
        person_link = ('https://www.linkedin.com/in/' + person_info_json["publicIdentifier"],)
        link_list_current_page.append(person_link)
    return link_list_current_page


def crawl_person_info(url, s, company_name_cn, company_name_en):
    try:
        failure = 0
        while failure < 10:
            try:
                r = s.get(url, timeout=20)
            except Exception as e:
                print(e)
                failure += 1
                continue
            if r.status_code == 200:
                person_school_list = parse_person_info(r.content, url, company_name_cn, company_name_en)
                return person_school_list
            else:
                print('%s %s' % (r.status_code, url))
                failure += 2
        if failure >= 10:
            print('Failed: %s' % url)
    except Exception as e:
        print(e)
        return -1


def parse_person_info(content, url, company_name_cn, company_name_en):
    global LINKED_COUNT
    LINKED_COUNT = LINKED_COUNT + 1
    print("正在读取第%s个员工信息" % LINKED_COUNT)
    content = urllib.parse.unquote(content.decode("utf-8"))
    content = content.replace('&quot;', '"')
    profile_txt = ''.join(re.findall('(\{[^\{]*?profile\.Profile"[^\}]*?\})', content))
    first_name = re.findall('"firstName":"(.*?)"', profile_txt)
    last_name = re.findall('"lastName":"(.*?)"', profile_txt)
    positions = re.findall('(\{[^\{]*?profile\.Position"[^\}]*?\})', content)
    educations = re.findall('(\{[^\{]*?profile\.Education"[^\}]*?\})', content)

    # 必须拥有姓名、company_name公司的工作经历、教育经历才进行爬取
    if first_name and last_name and positions and educations:
        work_in_company_name = False
        for one in positions:
            company_name_list = re.findall('"companyName":"(.*?)"', one)
            if company_name_list:
                if company_name_cn in one or company_name_en in one:
                    work_in_company_name = True
                    break
        if not work_in_company_name:
            #print("此人无company_name公司的工作经历")
            return

        print("正在解析员工的LinkedIn主页")
        print('姓名: %s%s    Linkedin: %s' % (last_name[0], first_name[0], url))
        if positions:
            print('工作经历:')
        for one in positions:
            company_name_list = re.findall('"companyName":"(.*?)"', one)
            title = re.findall('"title":"(.*?)"', one)
            location_name = re.findall('"locationName":"(.*?)"', one)
            time_period = re.findall('"timePeriod":"(.*?)"', one)
            position_time = ''
            if time_period:
                start_date_txt = ' '.join(re.findall(
                    '(\{[^\{]*?"\$id":"%s,startDate"[^\}]*?\})' % time_period[0].replace('(', '\(').replace(')', '\)'),
                    content))
                end_date_txt = ' '.join(re.findall(
                    '(\{[^\{]*?"\$id":"%s,endDate"[^\}]*?\})' % time_period[0].replace('(', '\(').replace(')', '\)'),
                    content))
                start_year = re.findall('"year":(\d+)', start_date_txt)
                start_month = re.findall('"month":(\d+)', start_date_txt)
                end_year = re.findall('"year":(\d+)', end_date_txt)
                end_month = re.findall('"month":(\d+)', end_date_txt)
                start_date = ''
                if start_year:
                    start_date += '%s' % start_year[0]
                    if start_month:
                        start_date += '.%s' % start_month[0]
                end_date = ''
                if end_year:
                    end_date += '%s' % end_year[0]
                    if end_month:
                        end_date += '.%s' % end_month[0]
                if len(start_date) > 0 and len(end_date) == 0:
                    end_date = '现在'
                position_time += '   %s ~ %s' % (start_date, end_date)
            if company_name_list:
                title = '   %s' % title[0] if title else ''
                location_name = '   %s' % location_name[0] if location_name else ''
                print('    %s %s %s %s' % (company_name_list[0], position_time, title, location_name))

        if educations:
            print('教育经历:')
        for one in educations:
            school_name = re.findall('"schoolName":"(.*?)"', one)
            field_of_study = re.findall('"fieldOfStudy":"(.*?)"', one)
            degree_name = re.findall('"degreeName":"(.*?)"', one)
            time_period = re.findall('"timePeriod":"(.*?)"', one)
            school_time = ''
            if time_period:
                start_date_txt = ' '.join(re.findall(
                    '(\{[^\{]*?"\$id":"%s,startDate"[^\}]*?\})' % time_period[0].replace('(', '\(').replace(')', '\)'),
                    content))
                end_date_txt = ' '.join(re.findall(
                    '(\{[^\{]*?"\$id":"%s,endDate"[^\}]*?\})' % time_period[0].replace('(', '\(').replace(')', '\)'),
                    content))
                start_year = re.findall('"year":(\d+)', start_date_txt)
                start_month = re.findall('"month":(\d+)', start_date_txt)
                end_year = re.findall('"year":(\d+)', end_date_txt)
                end_month = re.findall('"month":(\d+)', end_date_txt)
                start_date = ''
                if start_year:
                    start_date += '%s' % start_year[0]
                    if start_month:
                        start_date += '.%s' % start_month[0]
                end_date = ''
                if end_year:
                    end_date += '%s' % end_year[0]
                    if end_month:
                        end_date += '.%s' % end_month[0]
                if len(start_date) > 0 and len(end_date) == 0:
                    end_date = '现在'
                school_time += '   %s ~ %s' % (start_date, end_date)
            if school_name:
                field_of_study = '   %s' % field_of_study[0] if field_of_study else ''
                degree_name = '   %s' % degree_name[0] if degree_name else ''
                print('    %s %s %s %s' % (school_name[0], school_time, field_of_study, degree_name))

        summary = re.findall('"summary":"(.*?)"', profile_txt)
        if summary:
            print('简介: %s' % summary[0])

        occupation = re.findall('"headline":"(.*?)"', profile_txt)
        if occupation:
            print('身份/职位: %s' % occupation[0])

        location_name = re.findall('"locationName":"(.*?)"', profile_txt)
        if location_name:
            print('坐标: %s' % location_name[0])

        network_info_txt = ' '.join(re.findall('(\{[^\{]*?profile\.ProfileNetworkInfo"[^\}]*?\})', content))
        connections_count = re.findall('"connectionsCount":(\d+)', network_info_txt)
        if connections_count:
            print('好友人数: %s' % connections_count[0])
        website_txt = ' '.join(re.findall('("included":.*?profile\.StandardWebsite",.*?\})', content))
        website = re.findall('"url":"(.*?)"', website_txt)
        if website:
            print('个人网站: %s' % website[0])

        projects = re.findall('(\{[^\{]*?profile\.Project"[^\}]*?\})', content)
        if projects:
            print('所做项目:')
        for one in projects:
            title = re.findall('"title":"(.*?)"', one)
            description = re.findall('"description":"(.*?)"', one)
            time_period = re.findall('"timePeriod":"(.*?)"', one)
            project_time = ''
            if time_period:
                start_date_txt = ' '.join(re.findall('(\{[^\{]*?"\$id":"%s,startDate"[^\}]*?\})' % time_period[0].replace('(', '\(').replace(')', '\)'), content))
                end_date_txt = ' '.join(re.findall('(\{[^\{]*?"\$id":"%s,endDate"[^\}]*?\})' % time_period[0].replace('(', '\(').replace(')', '\)'), content))
                start_year = re.findall('"year":(\d+)', start_date_txt)
                start_month = re.findall('"month":(\d+)', start_date_txt)
                end_year = re.findall('"year":(\d+)', end_date_txt)
                end_month = re.findall('"month":(\d+)', end_date_txt)
                start_date = ''
                if start_year:
                    start_date += '%s' % start_year[0]
                    if start_month:
                        start_date += '.%s' % start_month[0]
                end_date = ''
                if end_year:
                    end_date += '%s' % end_year[0]
                    if end_month:
                        end_date += '.%s' % end_month[0]
                if len(start_date) > 0 and len(end_date) == 0:
                    end_date = '现在'
                project_time += '   时间: %s ~ %s' % (start_date, end_date)
            if title:
                print('    %s %s %s' % (title[0], project_time, '   项目描述: %s' % description[0] if description else ''))
        school_appended_list = []
        person_school_tuple_list = []
        for one in educations:
            school_name = re.findall('"schoolName":"(.*?)"', one)[0]
            if school_name and school_name in school_appended_list:
                continue
            else:
                school_appended_list.append(school_name)
                person_school_tuple_list.append((first_name[0].replace('\'', ''), last_name[0], school_name))
        return person_school_tuple_list


if __name__ == '__main__':
    #s = login("642150216@qq.com", "88288349a")
    #s = login("18622397632", "88288349a")
    s = login("861610921@qq.com", "88288349a")
    if s == -1:
        print("登陆失败，程序退出")
    else:
        print("登陆成功")
        company_name_en = input('将在MySQL数据库BATHEmployee中创建表（若不存在），请输入存入表名(英文):')
        db_conn = connect_database('localhost', 3306, 'root', 'yuklyn', 'BATHEmployee', company_name_en)
        company_name_cn = input('请输入爬取的公司名:')


        db_cursor = db_conn.cursor()
        # 目前只适配了百度，baidu
        url_research_root = 'https://www.linkedin.com/search/results/people'
        url_research_parameters = '/?facetCurrentCompany=%5B%2248433%22%5D' \
                                  '&facetGeoRegion=%5B%22cn%3A0%22%5D' \
                                  '&keywords=%E7%99%BE%E5%BA%A6' \
                                  '&origin=FACETED_SEARCH'
        url_research_page = '&page=%s'
        url_research = url_research_root + url_research_parameters
        page_count = 1
        # 抓取LINKEDIN_RESEARCH_MAX_PAGE页内的会员主页链接，存入数据库对应公司的Link表中
        person_url_total_list = []
        while(page_count <= LINKEDIN_RESEARCH_MAX_PAGE):
            link_list_current_page = get_person_url_per_page(url_research + url_research_page % page_count, copy.deepcopy(s))
            print(link_list_current_page)
            if link_list_current_page and link_list_current_page != -1:
                person_url_total_list[len(person_url_total_list):len(person_url_total_list)] = link_list_current_page
            page_count = page_count + 1
        # 去重
        print('去重前表长：%s' % person_url_total_list.__len__())
        person_url_total_list = list(set(person_url_total_list))
        print('去重后表长：%s' % person_url_total_list.__len__())

        sql = "INSERT INTO " + company_name_en + COMPANY_TABLE_LINK + "(url) VALUES(%s)"
        db_cursor = db_conn.cursor()
        db_cursor.executemany(sql, person_url_total_list)
        db_conn.commit()
        db_cursor.close()


        db_cursor = db_conn.cursor()
        i = 1
        sql = "SELECT url from " + company_name_en + COMPANY_TABLE_LINK + " where id between %s and %s" %(i, i+602)
        db_cursor.execute(sql)
        url_tuple_list = db_cursor.fetchall()
        for url_tuple in url_tuple_list:
            url = ''.join(list(url_tuple))
            school_tuple_list = crawl_person_info(url, copy.deepcopy(s), company_name_cn, company_name_en)
            time.sleep(2)
            print(school_tuple_list)
            sql = "INSERT INTO " + company_name_en + COMPANY_TABLE_EMPLOYEE + "(firstName, lastName, school) values(%s, %s, %s)"
            db_cursor.executemany(sql, school_tuple_list)
            db_conn.commit()
        db_cursor.close()



        """
        url_home = 'https://www.linkedin.com/nhome/'
        companyName = input('请输入爬取的公司名:')
        #browser = webdriver.PhantomJS()
        browser = webdriver.Chrome()
        browser.get(url_home)
        time.sleep(0.1)
        browser.delete_all_cookies()
        cookiesJar = s.cookies
        cookies = requests.utils.dict_from_cookiejar(cookiesJar)
        for k,v in cookies.items():
            browser.add_cookie({'domain': '.linkedin.com', 'httponly': False, 'name': k, 'path': '/', 'secure': False, 'value': v})
        time.sleep(0.1)
        browser.maximize_window()
        browser.get(url_home)
        time.sleep(0.1)
        elem_search_button = browser.find_element_by_class_name('nav-search-button')
        elem_search_content = browser.find_element_by_class_name('ember-text-field')
        elem_search_content.send_keys(companyName)
        elem_search_button.click()
        time.sleep(2)
        elem_location_button = browser.find_elements_by_class_name("search-facet__legend")[2]
        elem_location_button.click()
        time.sleep(1)

        elem_location_checkbox = browser.find_element_by_name("中国")
        elem_location_checkbox.click()
        time.sleep(5)

        elem_corporation_button = browser.find_elements_by_class_name("search-facet__legend")[3]
        elem_corporation_button.click()
        time.sleep(1)

        elem_corporation_checkbox = browser.find_element_by_name("Baidu, Inc.")
        elem_corporation_checkbox.click()
        time.sleep(3)

        elem_results_total = browser.find_element_by_class_name("search-results__total")
        print(elem_results_total.text)
        source = browser.page_source
        hrefs = re.findall(r'<a data-control-name="search_srp_result".*?href=.*?class="search-result__result-link ember-view">', source, re.I)
        hrefs = {'https://www.linkedin.com' + (href.split('href=')[1].split('id=')[0].replace('"', '').replace(' ', '')) for href in hrefs}
        hrefs = set(hrefs)
        print(hrefs)

        crawl(hrefs.pop(), copy.deepcopy(s))
        """


