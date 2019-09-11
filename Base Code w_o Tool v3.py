import multiprocessing
import time
import timeit
import csv
import re
from collections import deque
import requests
from bs4 import BeautifulSoup, Tag
from fake_useragent import UserAgent

# Setup Chrome display
# options = webdriver.ChromeOptions()
# options.add_argument('--ignore-certificate-errors')
# options.add_argument("--test-type")
ua = UserAgent()

Url = deque(["https://concordia.ab.ca/programs/all-programs/"])

# List of keywords for level
level_key = {'VOC': ['Cert', 'Certificate', 'Training', 'Practical'], 'DIP': ['Diploma', 'DIP'],
             'ADIP': ['Advance'],
             'FUG': ['Foundation', 'FD', 'FDs', 'FDEd', 'FdEd', 'FdA', 'FDA', 'FDArts', 'FDEng', 'FdSc', 'A.A.', 'A.S.',
                     'A.A.S', 'A.A.S.'],
             'UG': ['Bachelor', 'B.Trad.', 'BGInS', 'BAcc', 'BAFM', 'BAdmin', 'BAS', 'BAA', 'BASc', 'BASc/BEd',
                    'BArchSc', 'BAS', 'BA', 'BAS', 'BASc/BEd', 'BASc', 'BA/BComm', 'BA/BEd', 'BA/BEd/Dip', 'BA/LLB',
                    'BA/MA', 'BBRM', 'BBA', 'BBA/BA', 'BBA/BCS', 'BBA/BEd', 'BBA/BMath', 'BBA/BSc', 'BBE', 'BBTM', 'BDS',
                    'BCogSc', 'BComm', 'BCom', 'BCoMS', 'BCoSc', 'BCS', 'BComp', 'BCompSc', 'BCmp', 'BCFM', 'BDes', 'BDEM',
                    'BEcon', 'BEng', 'BEng&Mgt', 'BEngSoc', 'BESc', 'BEngTech', 'BEM', 'BESc', 'BESc/BEd', 'BES',
                    'BES/BEd', 'BFAA', 'BFA', 'BFA/BEd', 'BFS', 'BGBDA', 'BHP', 'BHSc', 'BHSc', 'BHS', 'BHS/BEd',
                    'BHum', 'BHK', 'BHRM', 'BID', 'BINF', 'BIT', 'BID', 'BIB', 'BJ', 'BJour', 'BJourn', 'BJHum',
                    'BKin', 'BK/BEd', 'BKI', 'BLA', 'BMOS', 'BMath', 'BMath/BEd', 'BMPD', 'BMRSc', 'BMSc', 'BMus',
                    'MusBac', 'BMusA', 'BMus/BEd', 'BMuth', 'BOR', 'BOR/BA', 'BOR/BEd', 'BOR/BSc', 'BPHE', 'BPhEd',
                    'BPHE/BEd', 'BPA', 'BPAPM', 'BPH', 'BRLS', 'BSc', 'BSc&Mgt', 'BSc/BASc', 'BSc/BComm', 'BSc/BEd',
                    'BSc(Eng)', 'BScFS', 'BScF', 'BSc(Kin)', 'BScN', 'BSocSc', 'BSW', 'BSE', 'BSM', 'BTech', 'BTh',
                    'BURPI', 'Degree', 'HND', 'HNC', 'iBA,iBA/BEd', 'iBBA', 'iBSc', 'iBSc/BEd', 'LLB', 'MEng', 'MChem',
                    'MPharm', 'MBiol', 'PCE', 'MBChB', 'DipHE', 'AS', 'BS', 'BSN', 'BSME', 'BSB', 'BSCH', 'BSA', 'BSEE',
                    'BLS', 'BSCE', 'BSCMPE', 'B.S.', 'B.A.', 'B.S.F.', 'B.M.', 'Major', 'Cognate', 'NSB', 'B.F.A.',
                    'Undergraduate'],
             'GPG': ['Graduate Diploma', 'GrDip'],
             'GPC': ['Graduate Certificate', 'PGCE', 'Grad Cert'],
             'PG': ['Master', 'MA', 'M.A.', 'A.M.', 'MPhil', 'M.Phil.', 'MSc', 'M.S.', 'SM', 'MBA', 'M.B.A.', 'MSci',
                    'LLM', 'LL.M.', 'MMath', 'MPhys', 'MPsych', 'MRes' 'MSci', 'LMHC', 'MSEd', 'MS', 'MSE', 'MSECE',
                    'DNP', 'MSME', 'M.Ed.', 'M.Eng.', 'M.C.T.L.', 'M.I.P.', 'M.F.A', 'M.P.A.', 'M.P.P.', 'M.F.A.', 'MASc',
                    'MCompSc', 'DEA', 'MEnv', 'MFA', 'MSCM',
                    'M.A.L.S.', 'M.P.H.', 'M.A.T.', 'M.S.T.', 'M.S.W.', 'M.I.C.L.J.', 'M.S.W.', 'MApCompSc', 'Graduate'],
             'DPG': ['Doctor', 'Doctorate', 'ClinPsyD', 'PhD', 'Ph.D.', 'D.D.', 'L.H.D.', 'Litt.D.', 'Ed.S.',
                     'LTD ', 'LL.D.', 'Mus.D.', 'S.D.', 'DPharm'],
             'Minor': ['Minor'],
             'SHORT COURSES': ['Short', 'Course'], 'SEMINAR': ['Seminar'], 'PRODEV': ['Tailor'], 'CONF': ['Conference'],
             'HONS': ['Honours', 'Hons']}

#      Day, Week, Month, Year
CLA = [24, 168, 720, 8760]  # Hours

# Duration Pattern Regex Library
dur_regex = [r"([0-9]+.years)", r"([0-9]+.year)",
             r"([0-9]+.Years)", r"([0-9]+.Year)",
             r"([0-9]+.month)", r"([0-9]+.Month)",
             r"([0-9]+.week)", r"([0-9]+.Week)"]

class Methods:
    # Check if the variable datatype is None
    def CheckNone(link):
        if link != None:
            return True
        else:
            return False

    # Check if the passed string contains http or https
    def HttpCheck(link):
        if Methods.CheckNone(link):
            if ('https://' in link) or ('http://' in link):
                return True
            else:
                return False
        else:
            return False

    # Check if link is Unique
    def Unique(link):
        with open('E:/Scrape/Canada/Concordia_ED/UniqueLinkList.csv', 'rt', encoding='utf-8') as Linklist:
            reader = csv.reader(Linklist)
            for url in reader:
                if Methods.CheckNone(link) & Methods.CheckNone(url):
                    if link == str(url).replace("['", '').replace("']", ''):
                        Linklist.close()
                        return False
                else:
                    Linklist.close()
                    return False
            return True

def cleanWord(word):
    return word.replace('(', ' ').replace(')', ' ').replace(',', ' ').replace('-', '').replace(':', '').replace('/', ' ')\
        .replace("'", ' ').replace(".", "")

def multi_pool(func, url, procs):             # Defines method to handle multiprocessing of collect_data()
    templist = []                                       # Stores the data to be returned from this method.
    counter = len(url)                                  # Number counter for total links left.
    pool = multiprocessing.Pool(processes=procs)
    print('Total number of processes: ' + str(procs))
    for a in pool.imap(func, url):                      # Loop each collect_data() execution.
        if a is not None:
            templist.append(a)                              # Puts the details row from collect_data() inside templist
            print('Number of links left: ' + str(counter - len(templist)))
    pool.terminate()
    pool.join()
    return templist

def convertLeast(x, y):
    return round(x * CLA[y - 1])


def convertNum(Text):
    return Text.replace('one', '1').replace('two', '2').replace('three', '3').replace('four', '4').replace('five', '5').replace('six', '6').replace('seven', '7').replace('eight', '8').replace('nine', '9')

def convertDuration(duration):
    duration = duration.lower()
    duration = convertNum(duration)
    numbers = re.findall(r'\d+(?:\.\d+)?', duration)

    dur_type_list = []
    for word in duration.split():
        if 'mester' in word.lower() or 'term' in word.lower() or 'hour' in word.lower() or 'day' in word.lower() or 'week' in word.lower() or 'month' in word.lower() or 'year' in word.lower():
            dur_type_list.append(word)

    nums = []
    for number in numbers:
        if number != '':
            nums.append(number)

    for number in nums:
        for dur in dur_type_list:
            if 'year' in dur:
                if '.' in number:
                    if re.findall(r'\d+', duration)[1] != 0:
                        return convertDuration(str(round(float(number) * 12)) + ' month')
                    return int(re.findall(r'\d+', duration)[0]), 'Years'
                else:
                    return int(number), 'Years'
            elif 'month' in dur:
                if '.' in number:
                    if re.findall(r'\d+', duration)[0] < 7:
                        return convertDuration(str(int(float(number) * 4)) + ' week')
                elif int(number) % 12 == 0:
                    return int(int(number) / 12), 'Years'
                else:
                    return int(round(float(number))), 'Months'
            elif 'week' in dur:
                return round(int(number)), ' Weeks'
            elif 'hour' in dur:
                return int(number), 'Hours'
            elif 'semester' in dur:
                return convertDuration(str(int(number) * 6) + 'month')
            elif 'trimester' in dur:
                return convertDuration(str(int(number) * 3) + 'month')
            elif 'term' in dur:
                return convertDuration(str(int(float(number)) * 6) + 'month')
            elif 'day' in dur:
                if '.' in number:
                    for jk in re.findall(r'\d+', duration):
                        if int(jk) > 1:
                            return convertDuration(str(int(float(number) * 24)) + 'hour')
                else:
                    return int(number), 'Days'
            else:
                return 'WRONG DATA'


# This function is to collect the links from the website.
def collect_links(link):
    # Only change the executable_path to your path. Leave the chrome_options.
    user = ua.random
    print(user)
    headers = {'User-Agent': user}

    req = requests.get(str(link), headers=headers)
    soup = BeautifulSoup(req.content, 'lxml')

    course_url_list = []
    homepage = 'https://www.concordia.ca'
    num = 0
    if soup.find('div', {'class': 'program-list'}) is not None:
        navigation = soup.find('div', {'class': 'program-list'})
        for a in navigation.find_all('div', {'class': 'table-cell'}):
            finder = a.find('a')
            if finder is not None:
                course_link = finder.get('href')
                with open('E:/Scrape/Canada/Concordia_ED/UniqueLinkList.csv', 'at', encoding='utf-8', newline='') as Linklist:
                    writer = csv.writer(Linklist)
                    if Methods.HttpCheck(course_link) & Methods.Unique(course_link):  # Check the link to make sure that it is correct and unique
                        u = (str(course_link).split('\n'))
                        writer.writerow(u)
                        course_url_list.append(u)
                        num += 1
                Linklist.close()
    print("================================\nAll " + str(num) + " the courses have been collected\n")

def collect_data(course_url_list):
    user = ua.random
    print(user)
    headers = {'User-Agent': user}

    # This will get the keywords from faculty file and put it into a dictionary
    with open('E:/Dropbox/Scrapping/Others/faculty.csv', 'rt', encoding='utf-8') as List:
        reader = csv.reader(List)
        mydict = {rows[0]: rows[1] for rows in reader}

    while True:
        num_loop = 1
        finished = 0
        retry_count = 1
        while finished != 1:
            try:
                if retry_count == 5:
                    print('This link cannot be opened ' + course_url_list)
                    break
                else:
                    time.sleep(1)  # buffer period
                    # print('OPENING ' + course_url_list)
                    # print('Retry Counter ' + str(retry_count))
                    req = requests.get(course_url_list, timeout=10, headers=headers)
                    if req.status_code == 404:
                        print(req.status_code)
                        req.raise_for_status()
                        break
                    if req:
                        finished = 1
            except requests.exceptions.HTTPError as e:
                print(e)
                print(course_url_list)
                retry_count += 1
                continue
            except requests.exceptions.ReadTimeout as e:
                print(e)
                print(course_url_list)
                retry_count += 1
            except requests.exceptions.ConnectionError as e:
                print(e)
                print(course_url_list)
                retry_count += 1
        soup = BeautifulSoup(req.content, "lxml")

        # details['Course Name', 'Level', 'Faculty', 'Duration', 'Duration Type', 'URL', 'Description', 'ScrapeAll']
        details = ['', '', '', '', '', '', '', '']

        # Course Name
        if soup.find('div', {'class': 'has-image'}) is not None:
            name = soup.find('div', {'class': 'has-image'}).find('h1').get_text()
            name = re.sub(r'[^\x00-\x7f]', r'', name)
            details[0] = " ".join(name.split())

        # Levels
        word = ''
        count = 0
        if soup.find('div', {'id': 'program-sidebar'}) is not None:
            a = soup.find('div', {'id': 'program-sidebar'}).find_all('p')
            if a is not None:
                if len(a) >= 2:
                    word = a[1].get_text()
        while details[1] == '':
            lock = 0
            if count == 1:
                word = cleanWord(details[0])
            if count == 2:
                details[1] = 'null'
                break
            for level, key in level_key.items():
                for each in key:
                    for wd in word.split(" "):
                        if each.lower() == wd.lower():  # Testing the equal, might change back to in
                            details[1] = level
                            lock = 1
                            break
                    if lock == 1:
                        break
                if lock == 1:
                    break
            count += 1

        # Faculty
        # Nothing to be changed here.
        loop_must_break = False
        for a in details[0].split():
            for fac, key in mydict.items():
                for each in key.split(','):
                    if each.replace("'", '').title() in a:
                        print("\t\t\t" + each + '  in  ' + details[0] + ' from ' + course_url_list)
                        details[2] = fac
                        loop_must_break = True
                        break
                    elif each.replace("'", '') in a.lower():
                        print("\t\t\t" + each + '  in  ' + details[0] + ' from ' + course_url_list)
                        details[2] = fac
                        loop_must_break = True
                        break
                if loop_must_break:
                    break
            if loop_must_break:
                break

        # Duration
        dur_found = 0
        if soup.find('div', {'id': 'program-sidebar'}) is not None:
            a = soup.find('div', {'id': 'program-sidebar'}).find_all('h4')
            if a is not None:
                for b in a:
                    dur_text = b.get_text()
                    for pattern in dur_regex:  # Locate Duration terms from Duration Regex Pattern Library, dur_regex.
                        if re.search(pattern, dur_text) is not None:
                            dur_text = str(re.findall(pattern, dur_text))
                            for c in dur_text.split(','):
                                if 'year' or 'month' or 'week' in c.lower():
                                    if convertDuration(c) != 'WRONG DATA' and convertDuration(c) is not None:
                                        details[3] = convertDuration(c)[0]
                                        details[4] = convertDuration(c)[1]
                                        dur_found = 1
                                        break
        if dur_found == 0:
            details[3] = 'null'
            details[4] = 'null'

        # URL
        details[5] = course_url_list

        # Description
        desc = ''
        desc_found = 0
        if soup.find('div', {'class': 'page-content'}) is not None:
            desc = soup.find('div', {'class': 'page-content'}).get_text()
            desc_found = 1
        if desc_found != 0:
            details[6] = " ".join(desc.split())
        else:
            details[6] = 'null'

        # Scrape All
        [s.extract() for s in soup(['style', 'script', '[document]', 'head', 'title'])]
        visible_text = repr(soup.get_text().replace(r'\\n', ' ').replace('\n', '').replace('\\', '').replace(', ', ''))
        visible_text = re.sub(r'[^\x00-\x7f]', r' ', visible_text)
        visible_text = ' '.join(visible_text.split())

        details[7] = str(repr(visible_text))
        print(details)
        num_loop += 1
        time.sleep(2)
        break
    if req.status_code != 404:
        return details


def main():
    start = timeit.default_timer()

    # Create the UniqueLinkList file.
    with open('E:/Scrape/Canada/Concordia_ED/UniqueLinkList.csv', 'wt') as Linklist:
        Linklist.close()

    while len(Url) != 0:
        collect_links(Url.pop())  # Call this to collect the links first.

    # Create an output file
    with open('E:/Scrape/Canada/Concordia_ED/ExtractedData.csv', 'wt', encoding='utf-8-sig', newline='') as website:
        writer = csv.writer(website)
        writer.writerow(['Course Name', 'Level', 'Faculty', 'Duration', 'Duration Type', 'URL', 'Description', 'ALLTXT'])

    # Get links from UniqueLinkList.csv
    with open('E:/Scrape/Canada/Concordia_ED/UniqueLinkList.csv', 'rt', encoding="utf-8") as course_link:
        reader = csv.reader(course_link)
        course_url_list = []
        for row in reader:
            course_url_list.append(row[0])
    print('Total number of Course Links: ' + str(len(course_url_list)))

    # Multiprocessing Collect_Data()
    # all_data should contain all 'details' that was returned from collect_data().
    all_data = multi_pool(collect_data, course_url_list, 10)            # Multiprocessing collect_data(). paramenters are (function name, iterable list, number of processes)
    with open('E:/Scrape/Canada/Concordia_ED/ExtractedData.csv', 'at', encoding="utf-8", newline='') as website:
        writer = csv.writer(website)
        print("Writing details to CSV File now....")
        for a in all_data:
            writer.writerow(a)
    print("Total number of rows written to ExtractedData.csv file: " + str(len(all_data)))

    stop = timeit.default_timer()
    time_sec = stop - start
    time_min = int(time_sec / 60)
    time_hour = int(time_min / 60)

    time_run = str(format(time_hour, "02.0f")) + ':' + str(
        format((time_min - time_hour * 60), "02.0f") + ':' + str(format(time_sec - (time_min * 60), "^-05.1f")))
    print("This code has completed running in: " + time_run)


if __name__ == '__main__':
    main()
