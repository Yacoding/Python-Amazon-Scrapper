from PyQt4 import QtGui
from PyQt4.QtCore import SIGNAL, Qt, QDir
from PyQt4.QtGui import *
import sys
from works.AmazonScrapper import AmazonScrapper

__author__ = 'Rabbi'


class Form(QMainWindow):
    def __init__(self, parent=None):
        super(Form, self).__init__(parent)
        self.createGui()
        self.fileName = None
        self.urlList = []

    def createGui(self):
        self.labelFile = QLabel('<b>Select File with url list: </b>')
        self.btnBrowse = QPushButton('&Browse')
        self.btnBrowse.clicked.connect(self.urlListSelected)

        self.labelUrl = QLabel('<b>URL: </b>')
        self.inputUrl = QLineEdit()

        self.labelCategory = QLabel('<b>Category: </b>')
        self.inputCategory = QLineEdit()

        self.btnScrap = QPushButton('&Scrap Amazon Data')
        self.btnScrap.clicked.connect(self.scrapAction)

        layout = QGridLayout()
        layout.addWidget(self.labelFile, 0, 0, Qt.AlignRight)
        layout.addWidget(self.btnBrowse, 0, 1, Qt.AlignLeft)
        layout.addWidget(self.labelUrl, 1, 0, Qt.AlignRight)
        layout.addWidget(self.inputUrl, 1, 1)
        layout.addWidget(self.labelCategory, 2, 0, Qt.AlignRight)
        layout.addWidget(self.inputCategory, 2, 1)
        layout.addWidget(self.btnScrap, 3, 1, Qt.AlignLeft)

        self.browser = QTextBrowser()
        layoutMain = QVBoxLayout()
        layoutMain.addLayout(layout)
        layoutMain.addWidget(self.browser)
        widget = QWidget()
        widget.setLayout(layoutMain)

        self.setCentralWidget(widget)
        screen = QDesktopWidget().screenGeometry()
        self.resize(screen.width() - 250, screen.height() - 250)
        self.setWindowTitle('Amazon Scraper.')

    def scrapAction(self):
        if len(str(self.inputCategory.text())) > 0:
            self.urlList.append(str(self.inputUrl.text()))
        self.amazon = AmazonScrapper(self.urlList, str(self.inputCategory.text()))
        self.amazon.start()
        self.amazon.notifyAmazon.connect(self.notifyInfo)

    def urlListSelected(self):
        self.fileName = QtGui.QFileDialog.getOpenFileName(self, "Select Text File", QDir.homePath() + "/Desktop")
        f = open(self.fileName, 'rb')
        self.lists = f.readlines()
        f.close()
        if self.lists is not None:
            for line in self.lists:
                self.urlList.append(line)


    def notifyInfo(self, data):
        try:
            self.browser.document().setMaximumBlockCount(1000)
            self.browser.append(data)
        except Exception, x:
            print x.message


class MainView:
    def __init__(self):
        pass

    def showMainView(self):
        app = QApplication(sys.argv)
        form = Form()
        form.show()
        sys.exit(app.exec_())
