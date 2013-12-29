import re
from PyQt4.QtCore import QThread, pyqtSignal
import time
from db.DbHelper import DbHelper
from logs.LogManager import LogManager
from utils.Csv import Csv
from utils.Utils import Utils
from spiders.Spider import Spider
from utils.Regex import Regex
from bs4 import BeautifulSoup

__author__ = 'Rabbi'


class AmazonScrapper(QThread):
    notifyAmazon = pyqtSignal(object)

    def __init__(self, urlList, category):
        QThread.__init__(self)
        self.logger = LogManager(__name__)
        self.spider = Spider()
        self.regex = Regex()
        self.utils = Utils()
        self.urlList = urlList
        self.category = category
        dupCsvReader = Csv()
        self.dupCsvRows = dupCsvReader.readCsvRow(category + '.csv')
        self.csvWriter = Csv(category + '.csv')
        csvDataHeader = ['SKU', 'Title', 'Sub Title', 'Price', 'Shipping Weight', 'Image URL']
        if csvDataHeader not in self.dupCsvRows:
            self.dupCsvRows.append(csvDataHeader)
            self.csvWriter.writeCsvRow(csvDataHeader)
        self.mainUrl = 'http://www.amazon.com'
        self.dupUrls = []
        self.total = len(self.dupCsvRows) - 1
        self.scrapUrl = None
        self.dbHelper = DbHelper('amazon.db')
        self.totalData = self.dbHelper.getTotalProduct()
        # self.productList = []

    def run(self):
        # self.scrapProductDetail(
        #     'http://www.amazon.com/Casio-G100-1BV-G-Shock-Classic-Ana-Digi/dp/B000AR7S3A/ref=sr_1_1/179-1326164-0664402?s=watches&ie=UTF8&qid=1387820165&sr=1-1',
        #     '', '', '', '')
        # return
        # print self.urlList
        # self.scrapReformatData('http://www.amazon.com/s/ref=sr_pg_3?rh=n%3A672123011%2Cn%3A%21672124011%2Cn%3A15743631%2Cn%3A3421075011&bbn=15743631&ie=UTF8&qid=1388303822&lo=shoes', 8)
        # return
        self.dbHelper.createTable('product')
        if self.urlList is not None and len(self.urlList):
            for url in self.urlList:
                if len(url) > 0:
                    url = self.regex.replaceData('(?i)\r', '', url)
                    url = self.regex.replaceData('(?i)\n', '', url)
                    self.notifyAmazon.emit('<font color=green><b>Amazon Main URL: %s</b></font>' % url)
                    imUrl = None
                    retry = 0
                    while imUrl is None and retry < 4:
                        imUrl = self.reformatUrl(url)
                        retry += 1
                    if imUrl is None:
                        imUrl = url
                    self.total = 0
                    print 'URL: ' + str(imUrl)
                    sortList = ['relevance-fs-browse-rank', 'price', '-price', 'reviewrank_authority', 'date-desc-rank']
                    for sort in sortList:
                        self.scrapReformatData(imUrl, sort)
                    self.notifyAmazon.emit(
                        '<font color=red><b>Finish data for Amazon Main URL: %s</b></font><br /><br />' % url)
        self.notifyAmazon.emit('<font color=red><b>Amazon Data Scraping finished.</b></font>')

    def reformatUrl(self, url):
        print url
        data = self.spider.fetchData(url)
        data = self.regex.reduceNewLine(data)
        data = self.regex.reduceBlankSpace(data)

        if data and len(data) > 0:
            soup = BeautifulSoup(data)
            if soup.find('span', class_='iltgl2 dkGrey').text.strip().lower() == 'Image'.lower():
                soup.clear()
                soup = None
                return url

            imageLink = [x.a.get('href') for x in soup.find_all('span', {'class': 'iltgl2'}) if x.a is not None]
            if imageLink is not None and len(imageLink) > 0 and imageLink[0] is not None and len(imageLink) > 0:
                imageLink = self.regex.replaceData('(?i)&amp;', '&', imageLink[0])
                print 'Image URL: ' + self.mainUrl + imageLink
                self.logger.debug('Image URL: ' + self.mainUrl + imageLink)
                return self.mainUrl + imageLink
        return None

    def scrapReformatData(self, url, sort='relevance-fs-browse-rank', page=1, retry=0):
        mainUrl = url + "&page=" + str(page) + "&sort=" + sort
        print 'Main URL: ' + mainUrl
        self.notifyAmazon.emit('<font color=green><b>Amazon URL: %s</b></font>' % mainUrl)
        data = self.spider.fetchData(mainUrl)
        if data and len(data) > 0:
            if self.scrapData(data, self.totalData) is False and retry < 4:
                self.notifyAmazon.emit('<Font color=green><b>Retry... as it gets less data than expected.</b></font>')
                del data
                # data = None
                return self.scrapReformatData(url, sort, page, retry + 1)

            soup = BeautifulSoup(data)
            if soup.find('a', id='pagnNextLink') is not None:
                del soup
                del data
                # soup = None
                # data = None
                return self.scrapReformatData(url, sort, page + 1, retry=0)

    def scrapData(self, data, totalCountForPage):
        data = self.regex.reduceNewLine(data)
        data = self.regex.reduceBlankSpace(data)
        results = None
        soup = BeautifulSoup(data)
        if len(soup.find_all('div', id=re.compile('^result_\d+$'))) > 0:
            print 'Total results div pattern: ' + str(len(soup.find_all('div', {'id': re.compile('^result_\d+$')})))
        elif len(soup.find_all('li', id=re.compile('^result_\d+$'))) > 0:
            results = soup.find_all('li', id=re.compile('^result_\d+$'))
            print 'Total results li pattern: ' + str(len(results))
            for productName in results:
                if self.dbHelper.searchProduct(productName.get('name').strip()) is False:
                    self.dbHelper.saveProduct(productName.get('name').strip())
                    self.totalData += 1
                else:
                    print 'Duplicate product found.'
                    self.notifyAmazon.emit(
                        '<font color=red><b>Duplicate product: [%s]</b></font>' % productName.get('name'))
                    # if productName.get('name') is not None and productName.get('name') not in self.productList:
                    #     self.productList.append(productName.get('name'))
            print 'Total products scrapped: ', str(self.totalData)
            self.notifyAmazon.emit('<font color=black><b>Total scrapped data: [%s]</b></font>' % str(self.totalData))
            del soup
            del data
            soup = None
            data = None
            return True

        if results is not None:
            print '\nScrapping from result page: '
            for result in results:
                self.scrapDataFromResultPage(result)

            if len(results) < totalCountForPage:
                return False
            else:
                return True
        return False
        # data = self.regex.reduceNewLine(data)
        # data = self.regex.reduceBlankSpace(data)
        #
        # dataChunks = None
        # if self.regex.isFoundPattern('(?i)<div id="result_[^"]*?"[^>]*?>(.*?)</ul>\s*?<br clear="all">', data):
        #     dataChunks = self.regex.getAllSearchedData(
        #         '(?i)<div id="result_[^"]*?"[^>]*?>(.*?)</ul>\s*?<br clear="all">', data)
        # elif self.regex.isFoundPattern('(?i)<li id="result_[^"]*?"[^>]*?>(.*?)</div> </li>', data):
        #     dataChunks = self.regex.getAllSearchedData('(?i)<li id="result_[^"]*?"[^>]*?>(.*?)</div> </li>', data)
        # elif self.regex.isFoundPattern('(?i)<div id="result_[^"]*?"[^>]*?>(.*?)</table>', data):
        #     dataChunks = self.regex.getAllSearchedData('(?i)<div id="result_[^"]*?"[^>]*?>(.*?)</table>', data)
        #
        # if dataChunks and len(dataChunks) > 0:
        #     self.totalData = len(dataChunks)
        #     self.total += len(dataChunks)
        #     self.notifyAmazon.emit('<b>Total Products Found [%s] For this category.</b>' % str(len(dataChunks)))
        #     self.notifyAmazon.emit('<b>Total Found [%s] For all.</b>' % str(self.total))
        #
        #     for dataChunk in dataChunks:
        #         try:
        #             dataChunk = self.regex.replaceData('(?i)&amp;', '&', dataChunk)
        #
        #             ## For scrap details url and title
        #             titleChunk = ''
        #             if self.regex.isFoundPattern(
        #                     '(?i)<h3 class="newaps"> <a href="([^"]*)"[^>]*?><span class="lrg bold"[^>]*>([^<]*)</span>',
        #                     dataChunk):
        #                 titleChunk = self.regex.getSearchedDataGroups(
        #                     '(?i)<h3 class="newaps"> <a href="([^"]*)"[^>]*?><span class="lrg bold"[^>]*?>([^<]*)</span>'
        #                     ,
        #                     dataChunk)
        #             elif self.regex.isFoundPattern('(?i)<a class="title" href="([^"]*)"[^>]*?>(.*?)</a>', dataChunk):
        #                 titleChunk = self.regex.getSearchedDataGroups(
        #                     '(?i)<a class="title" href="([^"]*)"[^>]*?>(.*?)</a>',
        #                     dataChunk)
        #
        #             if titleChunk:
        #                 ## Product Detail URL
        #                 url = titleChunk.group(1)
        #                 print 'url ' + url
        #                 url = self.regex.replaceData('(?i)&amp;', '&', url)
        #
        #                 ## Product Title
        #                 title = titleChunk.group(2)
        #                 title = self.regex.replaceData('(?i)&amp;', '&', title)
        #                 title = self.regex.replaceData('(?i)<span[^>]*>', '', title)
        #                 title = self.regex.replaceData('(?i)</span[^>]*>', '', title)
        #
        #                 ## Price of the Product
        #                 price = ''
        #                 if self.regex.isFoundPattern('<span class="bld lrg red">([^<]*)</span>', dataChunk):
        #                     price = self.regex.getSearchedData('<span class="bld lrg red">([^<]*)</span>', dataChunk)
        #                 elif self.regex.isFoundPattern('<td class="toeOurPrice"> <a href="[^"]*">([^<]*)</a></td>',
        #                                                dataChunk):
        #                     price = self.regex.getSearchedData(
        #                         '<td class="toeOurPrice"> <a href="[^"]*">([^<]*)</a></td>', dataChunk)
        #
        #                 ## SubTitle for Product
        #                 subTitle = ''
        #                 if self.regex.isFoundPattern('(?i)<span class="med reg"[^>]*?>\s*?by.*?</span>', dataChunk):
        #                     subTitle = self.regex.getSearchedData('(?i)<span class="med reg"[^>]*?>\s*?by(.*?)</span>'
        #                         , dataChunk)
        #                 elif self.regex.isFoundPattern('(?i)<span class="ptBrand">by.*?</span>', dataChunk):
        #                     subTitle = self.regex.getSearchedData('(?i)<span class="ptBrand">by(.*?)</span>', dataChunk)
        #                 subTitle = self.regex.replaceData('(?i)<a href=[^>]*?>', '', subTitle)
        #                 subTitle = self.regex.replaceData('(?i)</a>', '', subTitle)
        #                 subTitle = self.regex.replaceData('(?i) and', ',', subTitle)
        #
        #                 print 'title: ' + title
        #                 print 'subtitle: ' + subTitle
        #
        #                 if url.strip() not in self.dupUrls:
        #                     self.scrapProductDetail(url.strip(), title.strip(), subTitle.strip(), price.strip())
        #                     self.dupUrls.append(url.strip())
        #                 else:
        #                     self.notifyAmazon.emit(
        #                         '<font color=green><b>Already hit this url: [%s]. Skip it.</b></font>' % url.strip())
        #         except Exception, x:
        #             self.logger.error(x)
        #     if len(dataChunks) < totalCountForPage:
        #         return False
        # return True

    def scrapDataFromResultPage(self, data):
        if data and len(data) > 0:
            print data
            title = ''
            subTitle = ''
            price = ''
            image = ''
            url = ''

            ## Scrapping Title
            if data.find('span', class_='lrg bold') is not None:
                title = data.find('span', class_='lrg bold').text

            ## Scrapping Price
            if data.find('span', class_='red bld') is not None:
                price = data.find('span', class_='red bld').text
            elif data.find('span', class_='bld lrg red') is not None:
                price = data.find('span', class_='bld lrg red').text

            ## Scrapping Image
            if data.find('img', class_='ilo2 ilc2').get('src'):
                image = data.find('img', class_='ilo2 ilc2').get('src')

            ##Scrapping URL
            if data.find('h3', class_='newaps').find('a').get('href') is not None:
                url = data.find('h3', class_='newaps').find('a').get('href')
            print 'URL: ', url
            print 'Title: ' + title + ', Sub Title: ' + subTitle + ', Price: ' + price + ', Image: ' + image
            return self.scrapProductDetail(url, title, subTitle, price, image)
        return False

    def scrapProductDetail(self, url, title, subTitle, price, productImage):
        print 'Product URL: ', url
        self.notifyAmazon.emit('<font color=green><b>Product Details URL [%s].</b></font>' % url)
        data = self.spider.fetchData(url)
        if data and len(data) > 0:
            data = self.regex.reduceNewLine(data)
            data = self.regex.reduceBlankSpace(data)

            soup = BeautifulSoup(data, from_encoding='iso-8859-8')
            productSpec = None
            if soup.find('div', id='detailBullets_feature_div') is not None:
                productSpec = soup.find('div', id='detailBullets_feature_div').find_all('span', class_='a-list-item')

            ## Sub Title for Product
            if not subTitle or len(subTitle) == 0:
                if soup.find('a', id='brand') is not None:
                    subTitle = soup.find('a', id='brand').text
                elif soup.find_all('span', class_='author notFaded'):
                    print soup.find_all('span', class_='author notFaded')
                    subTitle = ', '.join([x.text for x in soup.find_all('span', class_='author notFaded')])
                elif self.regex.isFoundPattern('(?i)<span class="brandLink"> <a href="[^"]*?"[^>]*?>([^<]*)</a>',
                                               data):
                    subTitle = self.regex.getSearchedData(
                        '(?i)<span class="brandLink"> <a href="[^"]*?"[^>]*?>([^<]*)</a>', data)
                elif self.regex.isFoundPattern('(?i)<span >\s*?by[^<]*<a href="[^"]*"[^>]*?>([^<]*)</a>', data):
                    subTitle = self.regex.getSearchedData('(?i)<span >\s*?by[^<]*<a href="[^"]*"[^>]*?>([^<]*)</a>',
                                                          data)
                print "Sub TItle: " + subTitle

            ## SKU for Product
            sku = 'N/A'
            if productSpec is not None:
                sku = self.scrapProductSpec(productSpec, 'ASIN')
            elif self.regex.isFoundPattern('(?i)<li><b>ASIN:\s*?</b>([^<]*)<', data):
                skuChunk = self.regex.getSearchedData('(?i)<li><b>ASIN:\s*?</b>([^<]*)<', data)
                if skuChunk and len(skuChunk) > 0:
                    sku = skuChunk.strip()
            elif self.regex.isFoundPattern('(?i)<li><b>ISBN-13:</b>([^<]*)<', data):
                skuChunk = self.regex.getSearchedData('(?i)<li><b>ISBN-13:</b>([^<]*)<', data)
                if skuChunk and len(skuChunk) > 0:
                    sku = skuChunk.strip()
            print 'SKU: ', sku

            ## Shipping Weight for Product
            weight = 'N/A'
            if productSpec is not None:
                weight = self.scrapProductSpec(productSpec, 'Shipping Weight')
            elif self.regex.isFoundPattern('(?i)<li><b>Shipping Weight:</b>.*?</li>', data):
                weightChunk = self.regex.getSearchedData('(?i)(<li><b>Shipping Weight:</b>.*?</li>)', data)
                if weightChunk and len(weightChunk) > 0:
                    weightChunk = self.regex.replaceData('(?i)\(.*?\)', '', weightChunk)
                    weight = self.regex.getSearchedData('(?i)<li><b>Shipping Weight:</b>([^<]*)</li>', weightChunk)
                    weight = weight.strip()
            elif self.regex.getSearchedData('(?i)<li><b>\s*?Product Dimensions:\s*?</b>.*?</li>', data):
                weightChunk = self.regex.getSearchedData('(?i)(<li><b>\s*?Product Dimensions:\s*?</b>.*?</li>)', data)
                if weightChunk and len(weightChunk) > 0:
                    weightChunk = self.regex.replaceData('(?i)\(.*?\)', '', weightChunk)
                    weight = self.regex.getSearchedData('(?i)([0-9. ]+ounces)', weightChunk)
                    weight = weight.strip()
            print 'WEIGHT: ', weight

            images = self.scrapImages(data)
            print 'SCRAPED IMAGES: ', images
            image = ''
            if productImage is not None:
                image = productImage
            image += ', '.join(images)

            csvData = [sku, title, subTitle, price, weight, image]
            if csvData not in self.dupCsvRows:
                print csvData
                self.logger.debug(csvData)
                self.dupCsvRows.append(csvData)
                self.csvWriter.writeCsvRow(csvData)
                self.total += 1
                self.notifyAmazon.emit('<font color=red><b>All Products Scraped: [%s].</b></font>' % str(self.total))
                self.notifyAmazon.emit('<b>Scraped Data: %s</b>' % str(csvData))
            else:
                self.notifyAmazon.emit('<font color=green><b>Already exists this item. Skip it.</b></font>')

    def scrapProductSpec(self, data, pattern):
        dataChunk = [w for w in data if pattern in w.text]
        dataChunk = dataChunk[0].find_all('span') if dataChunk is not None and len(dataChunk) > 0 else None
        spec = dataChunk[1] if dataChunk is not None and len(dataChunk) > 1 else None
        spec = self.regex.replaceData('(?i)\(.*?\)', '', spec.text) if spec is not None else None
        return spec.strip() if spec is not None else 'N/A'

    def scrapImages(self, data):
        images = []
        ## If matched with this pattern
        if self.regex.isFoundPattern('(?i)<div id="thumb_\d+_inner"[^>]*?>.*?</div>', data):
            imageChunks = self.regex.getAllSearchedData('(?i)<div id="thumb_\d+_inner"[^>]*?>(.*?)</div>', data)
            if imageChunks and len(imageChunks) > 0:
                for imageChunk in imageChunks:
                    imageChunk = self.regex.getSearchedData('(?i)\((.*?)\)', imageChunk)
                    image = self.regex.getSearchedData('(?i)(http://ecx.images-amazon.com/images/I/[^.]*)\._.*?$'
                        , imageChunk)
                    if image and len(image) > 0:
                        image += '.jpg'
                        if image not in images:
                            images.append(image)

        ## Else if it matches with this pattern
        elif self.regex.isFoundPattern('(?i)<td class="tiny"><img.*?src="[^"]*"', data):
            imageChunks = self.regex.getAllSearchedData('(?i)<td class="tiny"><img.*?src="([^"]*)"', data)
            if imageChunks and len(imageChunks) > 0:
                for imageChunk in imageChunks:
                    if imageChunk and len(imageChunk) > 0:
                        image = self.regex.getSearchedData(
                            '(?i)(http://ecx.images-amazon.com/images/I/[^.]*)\._.*?$', imageChunk)
                        if image and len(image) > 0:
                            image += '.jpg'
                            if image not in images:
                                images.append(image)
        elif self.regex.isFoundPattern('(?i)<div id="main-image-wrapper-outer">(.*?)<div id="main-image-unavailable">',
                                       data):
            imageChunk = self.regex.getSearchedData(
                '(?i)<div id="main-image-wrapper-outer">(.*?)<div id="main-image-unavailable">', data)
            if imageChunk and len(imageChunk) > 0:
                mainImage = self.regex.getSearchedData('(?i)<img id="main-image" src="([^"]*)"', imageChunk)
                mainImage = self.regex.getSearchedData('(?i)(http://ecx.images-amazon.com/images/I/[^.]*)\._.*?$',
                                                       mainImage)

                if mainImage and len(mainImage) > 0:
                    mainImage += '.jpg'
                    if mainImage not in images:
                        images.append(mainImage)
                otherImages = self.regex.getAllSearchedData('(?i)src=\'(.*?)\'', imageChunk)
                if otherImages and len(otherImages) > 0:
                    for otherImage in otherImages:
                        otherImage = self.regex.getSearchedData(
                            '(?i)(http://ecx.images-amazon.com/images/I/[^.]*)\._.*?$', otherImage)
                        otherImage += '.jpg'
                        if otherImage not in images:
                            images.append(otherImage)

        elif self.regex.isFoundPattern('(?i)<h2 class="quorus-product-name">[^<]*</h2> <img src="([^"]*)"', data):
            imageChunk = self.regex.getSearchedData('(?i)<h2 class="quorus-product-name">[^<]*</h2> <img src="([^"]*)"',
                                                    data)
            if imageChunk and len(imageChunk) > 0:
                image = self.regex.getSearchedData('(?i)(http://ecx.images-amazon.com/images/I/[^.]*)\._.*?$',
                                                   imageChunk)
                if image and len(image) > 0:
                    image += '.jpg'
                    if image not in images:
                        images.append(image)

        elif self.regex.isFoundPattern(
                '(?i)<div customfunctionname="[^"]*" class="[^"]*" id="thumbs-image"[^>]*?>.*?</div>', data):
            imageChunks = self.regex.getSearchedData(
                '(?i)<div customfunctionname="[^"]*" class="[^"]*" id="thumbs-image"[^>]*?>(.*?)</div>', data)
            if imageChunks and len(imageChunks) > 0:
                images = self.regex.getAllSearchedData('(?i)src="(http://ecx.images-amazon.com/images/I/[^"]*)"'
                    , imageChunks)
                for image in images:
                    if image not in images:
                        images.append(image)
        elif self.regex.isFoundPattern('(?i)<div id="imageBlockContainer"[^>]*>(.*?)</div> </div></div>', data):
            imageChunks = self.regex.getSearchedData('(?i)<div id="imageBlockContainer"[^>]*>(.*?)</div> </div></div>',
                                                     data)
            if imageChunks and len(imageChunks) > 0:
                images = self.regex.getAllSearchedData('(?i)src="(http://ecx.images-amazon.com/images/I/[^"]*)"'
                    , imageChunks)
                for image in images:
                    if image not in images:
                        images.append(image)

        return images