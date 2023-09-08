from webdriver_manager.chrome import ChromeDriverManager
import os
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
path=ChromeDriverManager(path = r".//chrome_driver").install()
cwd = os.getcwd()
path = os.path.join(cwd, path)
config.set('CHROME', 'path', os.path.abspath(os.path.join(path, os.pardir)) + "\\chromedriver.exe")
# os.chdir(path)
with open('config.ini', 'w') as configfile:
    config.write(configfile)

