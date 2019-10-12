#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-

"""
    @description: This program can retrieve and save data in SQLite database,
    this data is alocate at https://guiaempresas.universia.es
    @author: Manuel Parra
    @license: MIT
    @date: 08/10/2019
    @modified: 08/10/2019
"""

from bs4 import BeautifulSoup
from modules.nettest import chargetest
import datetime as dt
import requests
import sys
import csv
import sqlite3
import re
import os
import time


def testingOurConnection():
    print("Testing the Internet connection, please wait!")
    host = ['8.8.8.8', '8.8.4.4']
    nt = chargetest(host)

    if not nt.isnetup():
        print("Your Internet connection is down!!, please try later!")
        sys.exit()

    print('The Ineternet connection is OK!...')


def makeTables(cursor):
    cursor.executescript('''
        CREATE TABLE IF NOT EXISTS UrlsUniversia (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            pos INTEGER,
            url_universia TEXT UNIQUE,
            revised INTEGER,
            row_date DATE,
            row_time DATE
        );

        CREATE TABLE IF NOT EXISTS Bussiness (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            id_url INTEGER ,
            name TEXT,
            address TEXT,
            location TEXT,
            province TEXT,
            phone1 TEXT,
            phone2 TEXT,
            cnae TEXT,
            webpage TEXT,
            row_date DATE,
            row_time DATE
        );
    ''')


def retrievehttp(link):
    print('Retrieving data from:', link)

    while True:
        try:
            res = requests.get(link)
            res.raise_for_status()
        except Exception as exc:
            print("There was a problem: %s" % (exc))
            print("Pausing for a bit...")
            time.sleep(5)
            print("Trying again, please wait...")
            continue

        if res.status_code != requests.codes.ok:
            print("We has a problem retrieving the data, check your code and" \
            "try again! Bye")
            sys.exit()

        return BeautifulSoup(res.text, 'html.parser')


def getInfo(cur, id_url, url):
    while True:
        soup = retrievehttp(url)

        tagName = soup.select('.h1ficha.fn.localbusiness')
        try:
            name = re.findall('>([\s\S]*)</', str(tagName[0]))[0]
        except:
            print("We has a problem to retrieving data, weiting 30 minits " \
            "to try again.!")
            time.sleep(1800)
            continue

        tagDirecction = soup.select('#situation_calle')
        direction = re.findall('>([\s\S]*)</', str(tagDirecction[0]))[0]

        tagLocality  = soup.select('#situation_loc')
        locality = re.findall('>([\s\S]*)</', str(tagLocality[0]))[0]

        tagProvince = soup.select('#situation_prov')
        province = re.findall('>([\s\S]*)</', str(tagProvince[0]))[0]

        tagTelephone = soup.select('table td[itemprop="telephone"]')
        telephone1 = None
        if tagTelephone != []:
            telephone1 = re.findall('>([\s\S]*)</', str(tagTelephone[0]))[0]

        tagsTh = soup.select_one('table').select('th')
        tagsTd = soup.select_one('table').select('td')

        telephone2 = None
        cnae = None
        for i, th in enumerate(tagsTh):
            if th.getText().strip() == 'Otros Teléfonos:':
                telephone2 = tagsTd[i].getText()
            elif th.getText().strip() == 'CNAE:':
                cnae = tagsTd[i].getText()

        if telephone2 != None:
            if  len(telephone2) > 18:
                telephone2 = telephone2[0:9] + '/' + telephone2[9:18] + '/' \
                + telephone2[18:27]
            elif len(telephone2) > 9:
                telephone2 = telephone2[0:9] + '/' + telephone2[9:18]

        web = None
        tagWeb = soup.select('#texto_ficha > p > a')
        if tagWeb != []:
            web = tagWeb[0].get('href')

        cur.execute("""
            INSERT OR IGNORE INTO Bussiness (id_url, name, address, location,
            province, phone1, phone2, cnae, webpage, row_date, row_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (id_url, name, direction, locality, province, telephone1, telephone2,
        cnae, web, dt.date.today(), dt.datetime.now().strftime("%H:%M:%S")))

        cur.execute("""
            UPDATE UrlsUniversia SET revised = 1
            WHERE id = ?
        """, (id_url, ))

        return cur


def exportDataCSV(loc):
    # call function main
    conn, cur, url, localidad = mainProcess(loc, False)

    cur.execute("""
        SELECT Bussiness.name, Bussiness.address, Bussiness.location,
        Bussiness.province, Bussiness.phone1, Bussiness.phone2, Bussiness.cnae,
        Bussiness.webpage, Bussiness.row_date, UrlsUniversia.url_universia
        FROM Bussiness JOIN UrlsUniversia ON Bussiness.id_url = UrlsUniversia.id
        WHERE Bussiness.location = ?
        ORDER BY UrlsUniversia.pos ASC, Bussiness.location
    """, (loc, ))
    rows = cur.fetchall()

    headList = ['Nombre', 'Dirección', 'Localidad', 'Provincia', 'Telefono 1',
                'Telefono 2', 'CNAE', 'Pagina Web', 'Fecha data', 'Fuente']

    dataBussiness = list()
    dataBussiness.append(headList)

    for row in rows:
        nam, add, loc, pro, tel1, tel2, cna, web, fec, hor = row
        dataBussiness.append([nam, add, loc, pro, tel1, tel2, cna, web, fec, hor])

    fname = 'file/bussiness.csv'

    print("Generando archivo csv con los datos en", fname)
    with open(fname, 'w') as file:
        write = csv.writer(file)
        write.writerows(dataBussiness)

    conn.close()

    print("Se exportaron", len(dataBussiness)-1, "registros." )
    print("Proceso finalizado!")
    input("Presione Enter para continuar...")


def mainProcess(loc, testInternetConn):
    # test our Internet connection
    if testInternetConn:
        testingOurConnection()

    # make the conn object
    conn = sqlite3.connect('bussines.sqlite')

    # connection to database fro storage data in it
    cur = conn.cursor()

    # if not exists make tables
    makeTables(cur)

    url = 'https://guiaempresas.universia.es'
    localidad = '/localidad/' + loc + '/'

    return conn, cur, url, localidad


def downloadLinks(loc):
    # call function main
    conn, cur, url, localidad = mainProcess(loc, True)

    dir = url + localidad

    # retrieving data from link
    soup = retrievehttp(dir)
    paginations = soup.select('div.pagination-centered a[href*="Pagina"]')

    if len(paginations) > 0:
        paginas  = int(paginations[len(paginations)-2].getText())
    else:
        paginas = 1

    # Reading links to bussiness
    # List for store bussiness
    count = 1
    dataList =  []

    for pag in range(1, (paginas+1), 1):
        links = soup.select('table.ranking_einf a')
        for link in links:
            dataList.append((count, url + link.get('href'), 0, dt.date.today(),
                dt.datetime.now().strftime("%H:%M:%S")))
            count += 1

        cur.executemany('''
            INSERT OR IGNORE INTO UrlsUniversia (pos, url_universia,
            revised, row_date, row_time)
            VALUES (?, ?, ?, ?, ?)
        ''', dataList)

        dataList = []
        if pag <= (paginas-1):
            dir = url + localidad + '?qPagina=' + str((pag + 1))
            soup = retrievehttp(dir)

    conn.commit()
    conn.close()

    print("Saving", (count-1), "company records at", localidad)
    print("Proceso finalizado!")
    input("Presione Enter para continuar...")


def downloadBussines():
    # call function main
    conn, cur, url, localidad = mainProcess('', True)

    cur.execute('''
        SELECT id, url_universia
        FROM UrlsUniversia
        WHERE revised = 0
        ORDER BY id
    ''')

    rows = cur.fetchall()

    if len(rows) < 1:
        print("No hay registros de empresas pendientes por descargar, " \
        "ejecute la opcion 1.")
        conn.close()
        return None

    print("Se inicia el proceso de dascarga de datos de", len(rows), \
    "registros de empresas empresas pendientes por descargar en base de datos")

    for row in rows:
        cur = getInfo(cur, row[0], row[1])
        conn.commit()

    print("Se registraron", len(rows), "empresas en la base de datos!")
    input("Presione Enter para continuar...")


def menu():
    while True:
        if sys.platform.lower()[:3] == 'lin':
            os.system('clear') # NOTA para windows tienes que cambiar clear por cls
        else:
            os.system('cls')

        print("Bienvenidos al sistema para descarga de datos de Universia!")
        print("--------------------------------" * 3, "\n")
        print("Menu de opciones:")
        print("1) Obtener los link de empresas por localidad")
        print("2) Obtener los datos de las empresas pendiente por descargar")
        print("3) Generar reporte de empresas por localidad")
        print("0) para salir del sistema\n")

        opcion = input("Por favor, presione 1, 2 o 3 segun la opción que desea," \
        "precione s para salir del sistema: ")

        if opcion == '0':
            print('Bye!')
            break
        elif opcion == '1':
            loc = input('Escriba la localidad: ')
            downloadLinks(loc)
        elif opcion == '2':
            downloadBussines()
        elif opcion == '3':
            loc = input('Escriba la localidad: ')
            print("Generando archivo CSV con los registros en base de datos.")
            exportDataCSV(loc)
        else:
            print("Opción invalida. Por favor, seleccione una opción valida.")
            input("Presione la tecla Enter para continuar...")


if __name__ == '__main__':
    menu()
