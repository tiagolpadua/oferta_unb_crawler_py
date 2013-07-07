#! /usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2
from lxml import etree
import sqlite3
import sys

URL_LISTA_CAMPUS = "http://www.matriculaweb.unb.br/matriculaweb/graduacao/oferta_campus.aspx"
URL_LISTA_DEPARTAMENTOS = "http://www.matriculaweb.unb.br/matriculaweb/graduacao/oferta_dep.aspx?cod=%s"
URL_LISTA_DISCIPLINAS = "http://www.matriculaweb.unb.br/matriculaweb/graduacao/oferta_dis.aspx?cod=%s"
URL_DETALHE_DISCIPLINA = "http://www.serverweb.unb.br/matriculaweb/graduacao/disciplina_pop.aspx?cod=%s"
URL_OFERTA_DISCIPLINA = "http://www.matriculaweb.unb.br/matriculaweb/graduacao/oferta_dados.aspx?cod=%s&dep=%s"
URL_LISTA_CURSO = "https://matriculaweb.unb.br/matriculaweb/graduacao/curso_rel.aspx?cod=%s"
URL_DETALHE_CURSO = "https://matriculaweb.unb.br/matriculaweb/graduacao/curso_dados.aspx?cod=%s"
URL_BASE_CURRICULO = "https://matriculaweb.unb.br/matriculaweb/graduacao/%s"

'''.//table[@class='FrameCinza']/tbody/tr[@bgcolor='white']/td/a/b|.//table[@class='FrameCinza']/tbody/tr[@bgcolor='#E7F3D6']/td/a/b'''

SQL_TABELA_CAMPUS = "CREATE TABLE campus (codigo INTEGER PRIMARY KEY, denominacao TEXT)"
class Campus:
  codigo = None
  denominacao = None
  
SQL_TABELA_DEPARTAMENTO = "CREATE TABLE departamento (codigo INTEGER PRIMARY KEY, sigla TEXT, denominacao TEXT, cod_campus INTEGER)"
class Departamento:
  codigo = None
  sigla = None
  denominacao = None
  cod_campus = None
  
SQL_TABELA_DISCIPLINA = "CREATE TABLE disciplina (codigo INTEGER PRIMARY KEY, denominacao TEXT, pre_requisitos TEXT, cod_departamento INTEGER, creditos TEXT)"
class Disciplina:
  codigo = None
  denominacao = None
  pre_requisitos = None
  creditos = None
  cod_departamento= None
  
SQL_TABELA_OFERTA = "CREATE TABLE oferta (codigo TEXT PRIMARY KEY, turma TEXT, vagas INTEGER, ocupado INTEGER, disponivel INTEGER, turno INTEGER, professor TEXT, cod_disciplina INTEGER)"
class Oferta:
  codigo = None
  turma = None
  vagas = None
  ocupado = None
  disponivel = None
  turno = None
  professor = None
  cod_disciplina = None
  
SQL_TABELA_HORARIO = "CREATE TABLE horario (codigo INTEGER PRIMARY KEY AUTOINCREMENT, dia_semana TEXT, local TEXT, hora_inicio TEXT, hora_fim TEXT, cod_oferta INTEGER)"
class Horario:
  codigo = None
  dia_semana = None
  local = None
  hora_inicio = None
  hora_fim = None
  cod_oferta = None
  
SQL_TABELA_CURSO = "CREATE TABLE curso (codigo INTEGER PRIMARY KEY, denominacao TEXT,  modalidade TEXT, turno TEXT, cod_campus INTEGER)"
class Curso:
  codigo = None
  denominacao = None
  modalidade = None
  turno = None
  cod_campus = None  
  
SQL_TABELA_CURRICULO = "CREATE TABLE curriculo (codigo INTEGER PRIMARY KEY AUTOINCREMENT, cod_curso INTEGER, cod_disciplina INTEGER)"
class Curriculo:
  codigo = None
  cod_curso = None
  cod_disciplina = None
  
def buscarCampus(con, customparser):
  con.execute("DROP TABLE IF EXISTS campus")
  con.execute(SQL_TABELA_CAMPUS)
  
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
  con.execute("DROP TABLE IF EXISTS departamento")
  con.execute(SQL_TABELA_DEPARTAMENTO)
  
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

def buscarDisciplinas(lista_departamento, con, customparser):
  con.execute("DROP TABLE IF EXISTS disciplina")
  con.execute(SQL_TABELA_DISCIPLINA)
  
  cur = con.cursor()
  
  for d in lista_departamento:
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
      lista_disciplina.append(c)
    
    for c in lista_disciplina:
      print "Obtendo pre-requisitos da disciplina: %s" % c.denominacao
      html = urllib2.urlopen(URL_DETALHE_DISCIPLINA % c.codigo).read()
      tree = etree.HTML(html, parser=customparser)
      trs = tree.xpath(".//tr[@class='padrao']")
      c.pre_requisitos = (trs[5][1].text + ''.join([etree.tostring(child) for child in trs[5][1].iterchildren()])).replace("<br/>"," ")
      print c.codigo, c.denominacao, c.pre_requisitos
     
    cur.executemany("INSERT INTO disciplina VALUES(?, ?, ?, ?, ?)", [(c.codigo, c.denominacao, c.pre_requisitos, c.cod_departamento, None) for c in lista_disciplina])
    con.commit()
    print "Ok"  
    
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

def buscarOfertas(lista_disciplina, con, customparser):
  con.execute("DROP TABLE IF EXISTS oferta")
  con.execute("DROP TABLE IF EXISTS horario")
  con.execute(SQL_TABELA_OFERTA)
  con.execute(SQL_TABELA_HORARIO)
  
  cur = con.cursor()
  
  indice = 0
  
  for d in lista_disciplina:
    indice+=1
    print "Obtendo a lista de ofertas da disciplina: %s - %s" % (d.codigo, d.denominacao)
    print "Iteração %s de %s" % (indice, len(lista_disciplina))
    try:
      html = urllib2.urlopen(URL_OFERTA_DISCIPLINA % (d.codigo, d.cod_departamento)).read()
      tree = etree.HTML(html, parser=customparser)
      td = tree.xpath(".//td[@width='30%']")
      d.creditos = td[0][3].text.strip()
      con.execute("UPDATE disciplina SET creditos='%s' WHERE codigo=%s" % (d.creditos, d.codigo))
      
      trs = tree.xpath(".//tr[@bgcolor='#FFFFFF']")
      lista_oferta = []
      lista_horario = []
      cont = 0
      for tr in trs:
        if len(tr.getchildren()) != 6:
          continue
        
        o = Oferta()
        o.cod_disciplina = d.codigo
        o.codigo = str(d.codigo) + tr[0][0][0][0].text
        o.turma = tr[0][0][0][0].text
        
        o.vagas = tr[1][0][0].text[7:]
        o.ocupado = tr[1][0][0][1].text
        o.disponivel = tr[1][0][0][3].text
        
        o.turno = tr[2][0][0].text
        o.professor = tr[4][0].text
        lista_oferta.append(o)
        
        tds = tree.xpath(".//td[@width=200]")
        i = 0
        while i < len(tds[(cont*2)+1]):
          if tds[(cont*2)+1][i].tag == "table":
            i+=1;
            continue
          
          elif tds[(cont*2)+1][i].tag == "div":
            h = Horario()
            h.cod_oferta = o.codigo
            h.dia_semana  = tds[(cont*2)+1][i][0].text
            h.hora_inicio = tds[(cont*2)+1][i][1][0].text
            h.hora_fim    = tds[(cont*2)+1][i][2].text
            h.local       = tds[(cont*2)+1][i][4][0].tail
            lista_horario.append(h)
            i+=1;
            continue
          
          elif tds[(cont*2)+1][i].tag == "b":
            h = Horario()
            h.cod_oferta = o.codigo
            h.dia_semana  = tds[(cont*2)+1][i].text
            h.hora_inicio = tds[(cont*2)+1][i+1][0].text
            h.hora_fim    = tds[(cont*2)+1][i+2].text
            h.local       = tds[(cont*2)+1][i+4][0].tail
            lista_horario.append(h)
            i+=5;
            continue
          
          else:
            print "Tag não identificada: ", tds[(cont*2)+1][i].tag
            sys.exit() 
          
        cont+=1
        
      cur.executemany("INSERT INTO oferta VALUES(?, ?, ?, ?, ?, ?, ?, ?)", 
                      [(o.codigo, o.turma, o.vagas, o.ocupado, o.disponivel, o.turno, o.professor, o.cod_disciplina) for o in lista_oferta])
  
      cur.executemany("INSERT INTO horario VALUES(?, ?, ?, ?, ?, ?)", 
                      [(None, h.dia_semana, h.local, h.hora_inicio, h.hora_fim, h.cod_oferta) for h in lista_horario])
    
      con.commit()
      print "Ok"  
    except:
      print "Unexpected error:", sys.exc_info()[0]
    
def lerOfertas(con):
  lista_oferta = []
  cur = con.cursor()
  cur.execute("SELECT * FROM oferta")
  rows = cur.fetchall()
  for row in rows:
    o = Oferta()
    o.codigo = row[0]
    o.turma = row[1]
    o.vagas = row[2]
    o.ocupado = row[3]
    o.disponivel = row[4]
    o.turno = row[5]
    o.professor = row[6]
    o.cod_disciplina = row[7]
    lista_oferta.append(o)
    
  return lista_oferta

def lerHorarios(con):
  lista_horario = []
  cur = con.cursor()
  cur.execute("SELECT * FROM horario")
  rows = cur.fetchall()
  for row in rows:
    h = Horario()
    h.codigo = row[0]
    h.dia_semana = row[1]
    h.local = row[2]
    h.hora_inicio = row[3]
    h.hora_fim = row[4]
    h.cod_oferta = row[5]
    lista_horario.append(h)
    
  return lista_horario

def buscarCursos(lista_campus, con, customparser):
  con.execute("DROP TABLE IF EXISTS curso")
  con.execute(SQL_TABELA_CURSO)
  
  for campus in lista_campus:
    print "Obtendo a lista de cursos do Campus: %s" % campus.denominacao
    html = urllib2.urlopen(URL_LISTA_CURSO % campus.codigo).read()
    tree = etree.HTML(html, parser=customparser)
    trs = tree.xpath(".//tr[@class='PadraoMenor']")
    lista_curso = []
    for tr in trs:
      c = Curso()
      c.modalidade = tr[0].text
      c.codigo = tr[1].text
      c.denominacao = tr[2][0].text
      c.turno = tr[3].text
      c.cod_campus = campus.codigo
      lista_curso.append(c)
     
    for c in lista_curso:
      print c.codigo, c.denominacao, c.modalidade, c.turno
     
    cur = con.cursor()
    cur.executemany("INSERT INTO curso VALUES(?, ?, ?, ?, ?)", [(c.codigo, c.denominacao, c.modalidade, c.turno, c.cod_campus) for c in lista_curso])
    con.commit()
    print "Ok"  
    
def lerCursos(con):
  lista_curso = []
  cur = con.cursor()
  cur.execute("SELECT * FROM curso")
  rows = cur.fetchall()
  for row in rows:
    c = Curso()
    c.codigo = row[0]
    c.denominacao = row[1]
    c.modalidade = row[2]
    c.turno = row[3]
    c.cod_campus = row[4]
    lista_curso.append(c)
    
  return lista_curso

def buscarCurriculos(lista_curso, con, customparser):
  con.execute("DROP TABLE IF EXISTS curriculo")
  con.execute(SQL_TABELA_CURRICULO)
  
  for curso in lista_curso:
    print "Obtendo o currículo para o curso: %s" % curso.denominacao
    html = urllib2.urlopen(URL_DETALHE_CURSO % curso.codigo).read()
    tree = etree.HTML(html, parser=customparser)
    link_curriculo = URL_BASE_CURRICULO % tree.xpath(".//tr[@class='PadraoMenor']/td[@align='right']/a")[1].attrib['href']
    
    print link_curriculo
    html = urllib2.urlopen(link_curriculo).read()
    tree = etree.HTML(html, parser=customparser)
    itens = tree.xpath(".//tr[@bgcolor='white']/td/a/b|.//tr[@bgcolor='#E7F3D6']/td/a/b")
    lista_curriculo = []
    for item in itens:
      c = Curriculo()
      c.cod_curso = curso.codigo
      c.cod_disciplina = item.text
      lista_curriculo.append(c)
     
    cur = con.cursor()
    cur.executemany("INSERT INTO curriculo VALUES(?, ?, ?)", [(None, c.cod_curso, c.cod_disciplina) for f in lista_curriculo])
    con.commit()
    print "Ok"  
    
def lerCurriculos(con):
  lista_curriculo = []
  cur = con.cursor()
  cur.execute("SELECT * FROM curriculo")
  rows = cur.fetchall()
  for row in rows:
    c = Curriculo()
    c.codigo = row[0]
    c.cod_curso = row[1]
    c.cod_disciplina = row[2]
    lista_curriculo.append(c)
    
  return lista_curriculo
    
def main():
  #################### VARIÁVEIS AUXILIARES ####################
  customparser = etree.HTMLParser(encoding="utf-8")
  isObterCampus = False
  isObterDepartamentos = False
  isObterDisciplinas = False
  isObterOfertas = False
  isObterCursos = False
  isObterCurriculos = False
  
  
  #################### INICIANDO CONEXÃO COM O BANCO DE DADOS ####################
  print "Iniciando conexão com o banco de dados...",
  con = sqlite3.connect("oferta_unb.db")
  print "Ok"
     
  #################### RESETANDO AS TABELAS DO BANCO ####################
  if isObterCampus:
    buscarCampus(con, customparser)
  
  lista_campus = lerCampus(con)
  
  if isObterDepartamentos:  
    buscarDepartamentos(lista_campus, con, customparser)
    
  lista_departamento = lerDepartamentos(con)
  
  if isObterDisciplinas:  
    buscarDisciplinas(lista_departamento, con, customparser)
    
  lista_disciplina = lerDisciplinas(con)
  
  if isObterOfertas:
    buscarOfertas(lista_disciplina, con, customparser)
    
  lista_oferta = lerOfertas(con)
  lista_horario = lerHorarios(con)
  
  if isObterCursos:
    buscarCursos(lista_campus, con, customparser)
    
  lista_curso = lerCursos(con)
  
  if isObterCurriculos:
    buscarCurriculos(lista_curso, con, customparser)
    
  lista_curriculo = lerCurriculos(con)
  
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