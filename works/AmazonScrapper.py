import re
from PyQt4.QtCore import QThread, pyqtSignal
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
        self.scrapUrl = None
        self.dbHelper = DbHelper('amazon.db')
        self.dbHelper.createTable(category)
        self.total = self.dbHelper.getTotalProduct(category)

    def run(self, retry=0):
        try:
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
                        sortList = ['relevance-fs-browse-rank', 'price', '-price', 'reviewrank_authority',
                                    'date-desc-rank']
                        for sort in sortList:
                            self.scrapReformatData(imUrl, sort)
                        self.notifyAmazon.emit(
                            '<font color=red><b>Finish data for Amazon Main URL: %s</b></font><br /><br />' % url)
            self.notifyAmazon.emit('<font color=red><b>Amazon Data Scraping finished.</b></font>')
        except Exception, x:
            print x.message
            self.logger.error('Exception at run: ', x.message)
            if retry < 5:
                self.run(retry + 1)

    def reformatUrl(self, url):
        try:
            print 'URL when reformat: ', url
            data = self.spider.fetchData(url)
            data = self.regex.reduceNewLine(data)
            data = self.regex.reduceBlankSpace(data)

            if data and len(data) > 0:
                soup = BeautifulSoup(data)
                imageLinkChunk = soup.find('span', class_='iltgl2 dkGrey')
                if imageLinkChunk is not None and imageLinkChunk.text is not None and imageLinkChunk.text.strip().lower() == 'Image'.lower():
                    del soup
                    return url

                imageLink = [x.a.get('href') for x in soup.find_all('span', {'class': 'iltgl2'}) if x.a is not None]
                if imageLink is not None and len(imageLink) > 0 and imageLink[0] is not None and len(imageLink) > 0:
                    imageLink = self.regex.replaceData('(?i)&amp;', '&', imageLink[0])
                    print 'Image URL: ' + self.mainUrl + imageLink
                    self.logger.debug('Image URL: ' + self.mainUrl + imageLink)
                    return self.mainUrl + imageLink
        except Exception, x:
            print x
            self.logger.error('Exception at reformat url: ', x.message)
        return None

    def scrapReformatData(self, url, sort='relevance-fs-browse-rank', page=1, retry=0):
        try:
            mainUrl = url + "&page=" + str(page) + "&sort=" + sort
            print 'Main URL: ' + mainUrl
            self.notifyAmazon.emit('<font color=green><b>Amazon URL: %s</b></font>' % mainUrl)
            data = self.spider.fetchData(mainUrl)
            if data and len(data) > 0:
                data = self.regex.reduceNewLine(data)
                data = self.regex.reduceBlankSpace(data)
                if self.scrapData(data) is False and retry < 4:
                    self.notifyAmazon.emit(
                        '<Font color=green><b>Retry... as it gets less data than expected.</b></font>')
                    del data
                    return self.scrapReformatData(url, sort, page, retry + 1)
                else:
                    print 'Problem scraping data'

                soup = BeautifulSoup(data)
                if soup.find('a', id='pagnNextLink') is not None:
                    del soup
                    del data
                    return self.scrapReformatData(url, sort, page + 1, retry=0)
        except Exception, x:
            print x.message
            self.logger.error('Exception at scrap reformat data: ', x.message)

    def scrapData(self, data):
        try:
            data = self.regex.reduceNewLine(data)
            data = self.regex.reduceBlankSpace(data)
            print data
            results = None

            soup = BeautifulSoup(data, from_encoding='ISO-8859-1')
            if len(soup.find_all('div', id=re.compile('^result_\d+$'))) > 0:
                print 'Total results div pattern: ' + str(len(soup.find_all('div', {'id': re.compile('^result_\d+$')})))
                results = soup.find_all('li', id=re.compile('^result_\d+$'))
            elif len(soup.find_all('li', id=re.compile('^result_\d+$'))) > 0:
                results = soup.find_all('li', id=re.compile('^result_\d+$'))
                print 'Total results li pattern: ' + str(len(results))

            if results is not None and len(results) > 0:
                for result in results:
                    productId = result.get('name').strip()
                    if self.dbHelper.searchProduct(productId, self.category) is False:
                        if self.scrapDataFromResultPage(result):
                            self.dbHelper.saveProduct(productId, self.category)
                    else:
                        print 'Duplicate product found [%s]' % productId
                        self.notifyAmazon.emit(
                            '<font color=red><b>Duplicate product: [%s]</b></font>' % productId)
                del data
                del soup
                return True
        except Exception, x:
            print x
            self.logger.error('Exception at scrap data: ', x.message)
        return False

    def scrapDataFromResultPage(self, data):
        try:
            if data and len(data) > 0:
                print 'Data: '
                print data
                title = ''
                subTitle = ''
                price = ''
                image = ''
                url = ''

                ## Scrapping Title
                if data.find('span', class_='lrg bold') is not None:
                    title = data.find('span', class_='lrg bold').text

                ## Scrapping SubTitle
                if self.regex.isFoundPattern('(?i)<span class="med reg"[^>]*?>\s*?by.*?</span>', data):
                    subTitle = self.regex.getSearchedData('(?i)<span class="med reg"[^>]*?>\s*?by(.*?)</span>', data)
                elif self.regex.isFoundPattern('(?i)<span class="ptBrand">by.*?</span>', data):
                    subTitle = self.regex.getSearchedData('(?i)<span class="ptBrand">by(.*?)</span>', data)

                if subTitle is not None:
                    subTitle = self.regex.replaceData('(?i)<a href=[^>]*?>', '', subTitle)
                    subTitle = self.regex.replaceData('(?i)</a>', '', subTitle)
                    subTitle = self.regex.replaceData('(?i) and', ',', subTitle)

                ## Scrapping Price
                if data.find('span', class_='red bld') is not None:
                    price = data.find('span', class_='red bld').text
                elif data.find('span', class_='bld lrg red') is not None:
                    price = data.find('span', class_='bld lrg red').text

                ## Scrapping Image
                if data.find('img', class_='ilo2 ilc2') is not None:
                    image = data.find('img', class_='ilo2 ilc2').get('src')
                elif data.find('img', class_='productImage ilc2') is not None:
                    image = data.find('img', class_='productImage ilc2').get('src')

                ##Scrapping URL
                if data.find('h3', class_='newaps').find('a').get('href') is not None:
                    url = data.find('h3', class_='newaps').find('a').get('href')

                if url is not None:
                    self.scrapProductDetail(url, title, subTitle, price, image)
                    return True
        except Exception, x:
            print x
            self.logger.error('Exception at scrap data from result page: ', x.message)
        return False

    def scrapProductDetail(self, url, title, subTitle, price, productImage):
        try:
            print 'Product URL: ', url
            self.notifyAmazon.emit('<font color=green><b>Product Details URL [%s].</b></font>' % url)
            data = self.spider.fetchData(url)
            if data and len(data) > 0:
                data = self.regex.reduceNewLine(data)
                data = self.regex.reduceBlankSpace(data)

                soup = BeautifulSoup(data, from_encoding='iso-8859-8')
                productSpec = None
                if soup.find('div', id='detailBullets_feature_div') is not None:
                    productSpec = soup.find('div', id='detailBullets_feature_div').find_all('span',
                                                                                            class_='a-list-item')

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
                    print "Sub Title: " + subTitle

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
                elif self.regex.isFoundPattern('(?i)<li>\s*?<b>Shipping Weight:</b>.*?</li>', data):
                    weightChunk = self.regex.getSearchedData('(?i)(<li><b>\s*?Shipping Weight\:</b>.*?</li>)', data)
                    if weightChunk and len(weightChunk) > 0:
                        weightChunk = self.regex.replaceData('(?i)\(.*?\)', '', weightChunk)
                        weight = self.regex.getSearchedData('(?i)<li>\s*?<b>\s*?Shipping Weight\:</b>([^<]*)</li>',
                                                            weightChunk)
                        weight = weight.strip()
                elif self.regex.isFoundPattern('(?i)<li>\s*?<b>\s*?Product Dimensions\:\s*?</b>.*?</li>', data):
                    weightChunk = self.regex.getSearchedData(
                        '(?i)(<li>\s*?<b>\s*?Product Dimensions\:\s*?</b>.*?</li>)',
                        data)
                    if weightChunk and len(weightChunk) > 0:
                        weightChunk = self.regex.replaceData('(?i)\(.*?\)', '', weightChunk)
                        weight = self.regex.getSearchedData('(?i)([0-9. ]+ounces)', weightChunk)
                        weight = weight.strip() if weight is not None else 'N/A'
                print 'WEIGHT: ', weight

                images = self.scrapImages(data)
                if productImage is not None and len(productImage) > 0:
                    productImage = self.regex.getSearchedData(
                        '(?i)(http://ecx.images-amazon.com/images/I/[^\.]*)\._.*?$', productImage)
                    if productImage and len(productImage) > 0:
                        productImage += '.jpg'
                        images.append(productImage)
                print 'SCRAPED IMAGES: ', images
                image = ', '.join(images)

                csvData = [sku, title, subTitle, price, weight, image]
                self.csvWriter.writeCsvRow(csvData)
                print csvData
                self.total += 1
                self.notifyAmazon.emit('<font color=black><b>All Products Scraped: [%s].</b></font>' % str(self.total))
                del data
                del soup
        except Exception, x:
            print x.message
            self.logger.error('Exception at scrap product detail: ', x.message)

    def scrapProductSpec(self, data, pattern):
        try:
            dataChunk = [w for w in data if pattern in w.text]
            dataChunk = dataChunk[0].find_all('span') if dataChunk is not None and len(dataChunk) > 0 else None
            spec = dataChunk[1] if dataChunk is not None and len(dataChunk) > 1 else None
            spec = self.regex.replaceData('(?i)\(.*?\)', '', spec.text) if spec is not None else None
            return spec.strip() if spec is not None else 'N/A'
        except Exception, x:
            print x
            self.logger.error('Exception at scrap product spec: ', x.message)
        return 'N/A'

    def scrapImages(self, data):
        images = []

        try:
            soup = BeautifulSoup(data)
            imageThumbs = soup.find_all('li', class_='a-spacing-small item')
            ## If matched with this pattern
            if imageThumbs is not None and len(imageThumbs) > 0:
                for imageChunk in soup.find_all('li', class_='a-spacing-small item'):
                    if imageChunk and len(imageChunk) > 0:
                        image = self.regex.getSearchedData(
                            '(?i)(http://ecx.images-amazon.com/images/I/[^\.]*)\._.*?$',
                            imageChunk.find('img').get('src'))
                        if image and len(image) > 0:
                            image += '.jpg'
                            if image not in images:
                                images.append(image)
            elif self.regex.isFoundPattern('(?i)<div id="thumb_\d+_inner"[^>]*?>.*?</div>', data):
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
            elif self.regex.isFoundPattern(
                    '(?i)<div id="main-image-wrapper-outer">(.*?)<div id="main-image-unavailable">',
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
                imageChunk = self.regex.getSearchedData(
                    '(?i)<h2 class="quorus-product-name">[^<]*</h2> <img src="([^"]*)"',
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
                imageChunks = self.regex.getSearchedData(
                    '(?i)<div id="imageBlockContainer"[^>]*>(.*?)</div> </div></div>',
                    data)
                if imageChunks and len(imageChunks) > 0:
                    images = self.regex.getAllSearchedData('(?i)src="(http://ecx.images-amazon.com/images/I/[^"]*)"'
                        , imageChunks)
                    for image in images:
                        if image not in images:
                            images.append(image)
        except Exception, x:
            print x
            self.logger.error('Exception at scrap images: ', x.message)

        return images