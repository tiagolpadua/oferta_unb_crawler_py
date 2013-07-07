#! /usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2
from lxml import etree
import sqlite3
from threading import Thread
from sqlalchemy.sql.expression import except_

URL_LISTA_CAMPUS = "http://www.matriculaweb.unb.br/matriculaweb/graduacao/oferta_campus.aspx";
URL_LISTA_DEPARTAMENTOS = "http://www.matriculaweb.unb.br/matriculaweb/graduacao/oferta_dep.aspx?cod=%s";
URL_LISTA_DISCIPLINAS = "http://www.matriculaweb.unb.br/matriculaweb/graduacao/oferta_dis.aspx?cod=%s";
URL_DETALHE_DISCIPLINA = "http://www.serverweb.unb.br/matriculaweb/graduacao/disciplina_pop.aspx?cod=%s";
URL_OFERTA_DISCIPLINA = "http://www.matriculaweb.unb.br/matriculaweb/graduacao/oferta_dados.aspx?cod=%s&dep=%s";

class Campus:
  codigo = None
  denominacao = None
  
class Departamento:
  codigo = None
  sigla = None
  denominacao = None
  cod_campus = None
  
class Disciplina:
  codigo = None
  denominacao = None
  pre_requisitos = None
  cod_departamento= None
  
def apagarTabelas(con):
  print "Apagando tabelas...",
  con.execute("DROP TABLE IF EXISTS campus");
  con.execute("DROP TABLE IF EXISTS departamento");
  con.execute("DROP TABLE IF EXISTS disciplina");
  con.execute("DROP TABLE IF EXISTS oferta");
  con.execute("DROP TABLE IF EXISTS horario");
  print "Ok"
  
  print "Criando tabelas...",
  con.execute("CREATE TABLE campus (codigo INTEGER PRIMARY KEY, denominacao TEXT)");
  con.execute("CREATE TABLE departamento (codigo INTEGER PRIMARY KEY, sigla TEXT, denominacao TEXT, cod_campus INTEGER)");
  con.execute("CREATE TABLE disciplina (codigo INTEGER PRIMARY KEY, denominacao TEXT, pre_requisitos TEXT, cod_departamento INTEGER)");
  con.execute("CREATE TABLE oferta (codigo INTEGER PRIMARY KEY, periodo TEXT, turma TEXT, turno INTEGER, professor TEXT, cod_disciplina INTEGER)");
  con.execute("CREATE TABLE horario (codigo INTEGER PRIMARY KEY, dia_semana INTEGER, local TEXT, hora_inicio INTEGER, hora_fim INTEGER, cod_oferta INTEGER);");
  print "Ok"
  
  con.commit()
  
def buscarCampus(con, customparser):
  con.execute("DELETE FROM campus");   
  print "Obtendo a lista de Campus da UnB..." 
  html = urllib2.urlopen(URL_LISTA_CAMPUS).read()
  tree = etree.HTML(html, parser=customparser)
  trs = tree.xpath(".//tr[@class='PadraoMenor']")
  lista_campus = []
  for tr in trs:
    c = Campus()
    c.codigo = tr[0].text
    c.denominacao = tr[1][0].text
    lista_campus.append(c)
  
  cur = con.cursor()
  cur.executemany("INSERT INTO campus VALUES(?, ?)", [(c.codigo, c.denominacao) for c in lista_campus])
  con.commit()
  print "Ok" 
  
def lerCampus(con):
  lista_campus = []
  cur = con.cursor()
  cur.execute("SELECT * FROM campus")
  rows = cur.fetchall()
  for row in rows:
    c = Campus()
    c.codigo = row[0]
    c.denominacao = row[1]
    lista_campus.append(c)
    
  return lista_campus  
  
def buscarDepartamentos(lista_campus, con, customparser):
  con.execute("DELETE FROM departamento");
  
  for campus in lista_campus:
    print "Obtendo a lista de departamentos do Campus: %s" % campus.denominacao
    html = urllib2.urlopen(URL_LISTA_DEPARTAMENTOS % campus.codigo).read()
    tree = etree.HTML(html, parser=customparser)
    trs = tree.xpath(".//tr[@class='PadraoMenor']")
    lista_departamento = []
    for tr in trs:
      d = Departamento()
      d.cod_campus = campus.codigo
      d.codigo = tr[0].text
      d.sigla = tr[1].text
      d.denominacao = tr[2][0].text
      lista_departamento.append(d)
     
    for d in lista_departamento:
      print d.codigo, d.sigla, d.denominacao
     
    cur = con.cursor()
    cur.executemany("INSERT INTO departamento VALUES(?, ?, ?, ?)", [(d.codigo, d.sigla, d.denominacao, d.cod_campus) for d in lista_departamento])
    con.commit()
    print "Ok"  
    
def lerDepartamentos(con):
  lista_departamento = []
  cur = con.cursor()
  cur.execute("SELECT * FROM departamento")
  rows = cur.fetchall()
  for row in rows:
    d = Departamento()
    d.codigo = row[0]
    d.sigla = row[1]
    d.denominacao = row[2]
    d.cod_campus = row[3]
    lista_departamento.append(d)
    
  return lista_departamento

def buscarDisciplinas(lista_departamento, con, customparser, incremental):
  if not incremental:
    con.execute("DELETE FROM disciplina");
  
  cur = con.cursor()
  
  for d in lista_departamento:
    try:
      print "Obtendo a lista de disciplinas do departamento: %s" % d.denominacao
      html = urllib2.urlopen(URL_LISTA_DISCIPLINAS % d.codigo).read()
      tree = etree.HTML(html, parser=customparser)
      trs = tree.xpath(".//tr[@class='PadraoMenor']")
      lista_disciplina = []
      for tr in trs:
        c = Disciplina()
        c.cod_departamento = d.codigo
        c.codigo = tr[0][0].text
        c.denominacao = tr[1][0].text
        
        if incremental:
          cur.execute("SELECT COUNT(*) FROM disciplina  WHERE codigo =%s" % c.codigo)
          if cur.fetchone()[0] == 0:
            lista_disciplina.append(c)
          else:
            print "Disciplina já incluída: ", c.codigo, c.denominacao
            
        else:
          lista_disciplina.append(c)
      
      for c in lista_disciplina:
        print "Obtendo pre-requisitos da disciplina: %s" % c.denominacao
        html = urllib2.urlopen(URL_DETALHE_DISCIPLINA % c.codigo).read()
        tree = etree.HTML(html, parser=customparser)
        trs = tree.xpath(".//tr[@class='padrao']")
        c.pre_requisitos = (trs[5][1].text + ''.join([etree.tostring(child) for child in trs[5][1].iterchildren()])).replace("<br/>"," ")
        print c.codigo, c.denominacao, c.pre_requisitos
       
      cur.executemany("INSERT INTO disciplina VALUES(?, ?, ?, ?)", [(c.codigo, c.denominacao, c.pre_requisitos, c.cod_departamento) for c in lista_disciplina])
      con.commit()
      print "Ok"  
    except:
      print "Ignorando"
    
def lerDisciplinas(con):
  lista_disciplina = []
  cur = con.cursor()
  cur.execute("SELECT * FROM disciplina")
  rows = cur.fetchall()
  for row in rows:
    d = Disciplina()
    d.codigo = row[0]
    d.denominacao = row[1]
    d.pre_requisitos = row[2]
    d.cod_departamento = row[3]
    lista_disciplina.append(d)
    
  return lista_disciplina
    
def main():
  #################### VARIÁVEIS AUXILIARES ####################
  customparser = etree.HTMLParser(encoding="utf-8")
  isApagarTabelas = False
  isObterCampus = False
  isObterDepartamentos = False
  isObterDisciplinas = True
  
  #################### INICIANDO CONEXÃO COM O BANCO DE DADOS ####################
  print "Iniciando conexão com o banco de dados...",
  con = sqlite3.connect("oferta_unb.db")
  print "Ok"
     
  #################### RESETANDO AS TABELAS DO BANCO ####################
  if isApagarTabelas:
    apagarTabelas(con)
    
  if isApagarTabelas or isObterCampus:
    buscarCampus(con, customparser)
  
  lista_campus = lerCampus(con)
  
  if isApagarTabelas or isObterDepartamentos:  
    buscarDepartamentos(lista_campus, con, customparser)
    
  lista_departamento = lerDepartamentos(con)
  
  if isApagarTabelas or isObterDisciplinas:  
    incremental = True
    buscarDisciplinas(lista_departamento, con, customparser, incremental)
     
  lista_disciplina = lerDisciplinas(con)
  
  #################### ENCERRANDO A CONEXÃO COM O BANCO DE DADOS ####################
  try:
    print "Encerrando conexão com o banco de dados...",
    con.commit()
    con.close()
    print "Ok"
  except sqlite3.Error as e:
    print "Erro: %s"  % e.args[0]
  
  print "Programa finalizado com sucesso"

if __name__ == "__main__":
  main()