import time
import pandas as pd
from bs4 import BeautifulSoup as bs

from sqlalchemy import create_engine

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from extras import printProgressBar, timeLog

def main():
    timeLog("Inicia carga del navegador...")
    # define la configuración del driver de navegación
    chromedriver=r"./assets/drive/chromedriver.exe"
    chrome_options = Options()  
    chrome_options.add_argument("--headless")  
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")

    driver=webdriver.Chrome(executable_path=chromedriver,options=chrome_options)

    # carga la página de concursos anid
    driver.get("https://www.anid.cl/concursos")

    try:
        # selecciona los concursos abiertos
        element = driver.find_element_by_xpath("//*[@id='app']/div[1]/div/div[2]/div[2]/ul/li[2]/div")
        element.click()

        timeLog("Inicia búsqueda de concursos abiertos en 'anid.cl'...")
        # loop para esperar a que la página cargue
        while True:
            try:
                pageList=driver.find_element_by_xpath("//*[@id='app']/div[2]/div[3]/ul").get_attribute('innerHTML')
                break
            except:
                time.sleep(1)

        pageList_parsed=bs(pageList, 'html.parser').find_all("li")

        concursoArray=[]
        # recorre todas las páginas con resultados
        for i in range(len(pageList_parsed)):
            driver.find_element_by_xpath(f"//*[@id='app']/div[2]/div[3]/ul/li[{i+1}]").click()
            # loop para esperar a que la página cargue
            while True:
                try:
                    concursoList=driver.find_element_by_xpath("//*[@id='app']/div[2]/div[2]").get_attribute('innerHTML')
                    break
                except:
                    time.sleep(1)
            
            concursoList_parsed=bs(concursoList, 'html.parser').find_all("div", class_="col-12 col-md-6 col-sm-12 p-2 mt-2")

            for concurso in concursoList_parsed:
                concursoSubDire=concurso.find_all("div",class_="text-uppercase min-font py-2")[0].text
                concursoNombre=concurso.find_all("a",class_="py-2 td-none font-weight-bold text-dark")[0].text
                concursoInicio=concurso.find_all("small")[0].text.replace("Inicio: ","")[:10]
                concursoFin_prev=concurso.find_all("small")[1].text.replace("Cierre: ","")[:10]
                concursoFin = concursoFin_prev if concursoFin_prev[1].isnumeric() else "31-12-2099"
                record=[concursoSubDire,concursoNombre,concursoInicio,concursoFin]
                concursoArray.append(record)

    finally:
        # cierra el navegador una vez que ya ha terminado
        driver.quit()

    timeLog("Revisa si existen concursos abiertos nuevos...")
    # crea un dataframe con la información de los concursos
    currentConcursoDataFrame=pd.DataFrame(concursoArray,columns=['SubDireccion','Concurso','Inicio','Fin'])
    currentConcursoDataFrame['Inicio']=pd.to_datetime(currentConcursoDataFrame['Inicio'], format='%d-%m-%Y')
    currentConcursoDataFrame['Fin']=pd.to_datetime(currentConcursoDataFrame['Fin'], format='%d-%m-%Y')
    currentConcursoDataFrame=currentConcursoDataFrame.astype(dtype={'SubDireccion':"string",'Concurso':"string",'Inicio':"datetime64[ns]",'Fin':"datetime64[ns]"})
    
    backupsPath="./backups"
    try:
        previousConcursoDataFrame = pd.read_pickle(f"{backupsPath}/HistoConcursosAbiertos.pkl")

        deltaConcursosDataFrame=pd.merge(previousConcursoDataFrame,currentConcursoDataFrame, how='outer', indicator='Control')
        deltaConcursosDataFrame=deltaConcursosDataFrame.loc[deltaConcursosDataFrame['Control'] == 'right_only']
        deltaConcursosDataFrame=deltaConcursosDataFrame[deltaConcursosDataFrame.columns[:-1]]

    except FileNotFoundError:
        # si no existe un archivo histórico considera a la extracción como el nuevo historico
        timeLog("!!!No se encontró un archivo historico, se utilizará la extracción actual completa...")
        deltaConcursosDataFrame=currentConcursoDataFrame

    if not (deltaConcursosDataFrame.empty):

        timeLog(f"Se encontraron {len(deltaConcursosDataFrame)} registros nuevos...")
        
        try:
            # Database settings
            dbpath = "./db/dbConcursosANID.db"
            engine = create_engine(f"sqlite:///{dbpath}", echo=False)
            sqlite_connection = engine.connect()
            
            deltaConcursosDataFrame.to_sql("ConcursosAbiertosANID",sqlite_connection,if_exists="append",index=False)
            currentConcursoDataFrame.to_pickle(f"{backupsPath}/HistoConcursosAbiertos.pkl")
        except Exception as err:
            timeLog(f"Ha ocurrido un error y el proceso no pudo finalizar. {err}")

        timeLog("Todos los registros fueron cargados exitosamente.")
    else:
        timeLog("No se encontró ningún concurso nuevo. El proceso terminó sin realizar ningún cambio.")

if __name__ == "__main__":
    main()