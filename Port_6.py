import sqlite3
import pandas as pd

def DropTable(con):
    try:
        cur = con.cursor()
        cur.execute("""DROP TABLE IF EXISTS pacientes""")
        cur.execute("""DROP TABLE IF EXISTS atendimento""")
        cur.execute("""DROP TABLE IF EXISTS atendimento_servico""")
        cur.execute("""DROP TABLE IF EXISTS servico""")
        cur.execute("""DROP TABLE IF EXISTS medicos_especialidades""")
        cur.execute("""DROP TABLE IF EXISTS medicos""")
        cur.execute("""DROP TABLE IF EXISTS especialidades""")
        cur.execute("""DROP TABLE IF EXISTS servicos_especialidades""")
        cur.execute("""DROP TABLE IF EXISTS tipo_servico""")
        cur.execute("""DROP TABLE IF EXISTS frequencias""")
    except Exception as e:
        print("O ocorreu o erro durante o drop table:",e)
    else:
        cur.close()

def CriarPacientes (con):
    try:
        cur = con.cursor()
        cur.execute("""CREATE TABLE pacientes (
        ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        Nome TEXT NOT NULL,
        RG VARCHAR(9) NOT NULL,
        CPF VARCHAR(11) NOT NULL,
        Nasc DATE NOT NULL,
        Sexo CHAR NOT NULL
        );
        """)
    except Exception as e:
        print("ocorreu o erro durante criar tabelas:",e)
    else:
        cur.close()

def CriarAtendimento(con):
    try:
        cur = con.cursor()
        cur.execute("""CREATE TABLE atendimento (
        ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        Paciente_ID INT NOT NULL,
        Data_Atendimento TEXT NOT NULL,
        Peso FLOAT NOT NULL,
        Altura FLOAT NOT NULL,
        Descricao TEXT NOT NULL,
        Codigo_Manchester INT,
        FOREIGN KEY (Paciente_ID) REFERENCES pacientes(ID)
        );
        """)
    except Exception as e:
        print("ocorreu o Erro durante criar atendimento:",e)
    else:
        cur.close()

def CriarAtendimento_Servico(con):
    try:
        cur = con.cursor()
        cur.execute("""CREATE TABLE atendimento_servico (
        Atendimento_ID INTEGER NOT NULL ,
        Servico_ID INTEGER NOT NULL,
        Data DATE NOT NULL,
        Valor_do_servico FLOAT,
        Medicos_ID INT NOT NULL,
        FOREIGN KEY (Medicos_ID) REFERENCES medicos(ID),
        FOREIGN KEY (Atendimento_ID) REFERENCES atendimento(ID),
        FOREIGN KEY (Servico_ID) REFERENCES servico(ID),
        PRIMARY KEY (Atendimento_ID,Servico_ID,Data)
        );
        """)
    except Exception as e:
        print("Ocorreu o erro criar atendimento servico:",e)
    else:
        cur.close()

def CriarServico(con):
    try:
        cur = con.cursor()
        cur.execute("""CREATE TABLE servico (
        ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        Codigo_TUSS VARCHAR(45) NOT NULL,
        Descricao VARCHAR(45) NOT NULL,
        Valor FLOAT NOT NULL,
        Tipo_servico_ID INTEGER NOT NULL,
        FOREIGN KEY (Tipo_servico_ID) REFERENCES tipo_servico(ID)
        );
        """)
    except Exception as e:
        print("Ocorreu o erro durante criar servico:",e)
    else:
        cur.close()

def CriarMedicoEspecialidades(con):
    try:
        cur = con.cursor()
        cur.execute("""CREATE TABLE medicos_especialidades (
        Medicos_ID INTEGER NOT NULL,
        Especialidades_ID INTEGER NOT NULL,   
        FOREIGN KEY (Especialidades_ID) REFERENCES especialidades(ID),
        FOREIGN KEY (Medicos_ID) REFERENCES medicos(ID) 
        );
        """)
    except Exception as e:
        print("ocorreu o erro durante criar medicos especialidades:",e)
    else:
        cur.close()

def CriarMedicos(con):
    try:
        cur = con.cursor()
        cur.execute("""CREATE TABLE medicos(
        ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        Nome VARCHAR(100) NOT NULL,
        CRM VARCHAR(10)
        );
        """)
    except Exception as e:
        print("ocorreu o erro durante criar medico",e)
    else:
        cur.close()

def CriarEspecialidades(con):
    try:
        cur = con.cursor()
        cur.execute("""CREATE TABLE especialidades (
        ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        Nome VARCHAR(45) NOT NULL,
        CID10_CAT VARCHAR(8) NOT NULL
        );
        """)
    except Exception as e:
        print("Ocorreu o erro durante criar especialidades:",e)
    else:
        cur.close()

def CriarServicosEspecialidades(con):
    try:
        cur = con.cursor()
        cur.execute("""CREATE TABLE servicos_especialidades (
        Servico_ID INTEGER NOT NULL,
        Especialidades_ID INTEGER NOT NULL,
        FOREIGN KEY (Servico_ID) REFERENCES servico(ID),
        FOREIGN KEY (Especialidades_ID) REFERENCES especialidades(ID),
        PRIMARY KEY (Servico_ID,Especialidades_ID)
        );
        """)
    except Exception as e:
        print("ocorreu o erro durante criar servico especialidades:",e)
    else:
        cur.close()

def CriarFrequencias(con):
    try:
        cur = con.cursor()
        cur.execute("""CREATE TABLE frequencias (
        Servico_ID int NOT NULL PRIMARY KEY,
        Sexo VARCHAR(1),
        QtdePeriodo INTEGER NOT NULL,
        PeriodoMeses INTEGER NOT NULL,
        IdadeMin INTEGER NOT NULL,
        IdadeMax INTEGER NOT NULL
        );
        """)
    except Exception as e:
        print("ocorreu o erro durante criar frequancias:",e)
    else:
        cur.close()

def CriarTipoServico(con):
    try:
        cur = con.cursor()
        cur.execute("""CREATE TABLE tipo_servico (
        ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        Tipo_Servico VARCHAR(45)
        );
        """)
    except Exception as e:
        print("Ocorreu o erro durante criar tipo servico:",e)
    else:
        cur.close()

def AdicionarPacientes(nome,rg,cpf,nasc,sexo,con):
    try:
        cur = con.cursor()
        cur.execute(""" INSERT INTO pacientes (nome,rg,cpf,nasc,sexo)
        VALUES (?,?,?,?,?)""",(nome,rg,cpf,nasc,sexo))
    except Exception as e:
        con.rollback()
        print("Ocorreu o erro ao adicionar paciente:",e)
    else:
        con.commit()
        cur.close()

def AdicionarAtendimentos(id_pac,data_at,peso,altura,desc,Manchester,con):
    try:
        cur = con.cursor()
        cur.execute(""" INSERT INTO atendimento (Paciente_ID,Data_Atendimento,Peso,Altura,Descricao,Codigo_Manchester) 
        VALUES (?,?,?,?,?,?)""",(id_pac,data_at,peso,altura,desc,Manchester))
    except Exception as e:
        con.rollback()
        print("Ocorreu o erro ao adicionar atendimento:",e)
    else:
        con.commit()
        cur.close()        

def AdicionarTipoServico(tipo_serv,con):
    try:
        cur = con.cursor()
        cur.execute("""INSERT INTO tipo_servico (Tipo_servico) 
        VALUES (?);""",(tipo_serv,))
    except Exception as e:
        con.rollback()
        print("Ocorreu o erro ao adicionar tipo servico:",e)
    else:
        con.commit()
        cur.close()

def AdicionarAtendimentos_servico(at_id,serv_Id,data,med_id,con):
    try:
        cur = con.cursor()
        cur.execute(""" INSERT INTO atendimento_servico (Atendimento_ID,Servico_ID,Data,Medicos_id) 
        VALUES (?,?,?,?)""",(at_id,serv_Id,data,med_id))
        cur.execute("""UPDATE atendimento_servico 
        SET Valor_do_servico = (SELECT servico.Valor FROM servico WHERE servico.ID = atendimento_servico.Servico_ID)
        WHERE Atendimento_ID = ?
        AND Servico_ID = ?
        AND Data = (?);""",(at_id,serv_Id,data))
    except Exception as e:
        con.rollback()
        print("Ocorreu o erro ao adicionar atendimento servico:",e)
    else:
        con.commit()
        cur.close()

def AdicionarServico(TUSS,Desc,Valor,tipo_serv,con):
    try:
        cur = con.cursor()
        cur.execute(""" INSERT INTO servico (Codigo_TUSS,Descricao,Valor,Tipo_servico_ID) 
        VALUES (?,?,?,?)""",(TUSS,Desc,Valor,tipo_serv))
    except Exception as e:
        con.rollback()
        print("Ocorreu o erro ao adicinar servico:",e)
    else:
        con.commit()
        cur.close()

def AdicionarEspecialidade(nome,cid,con):
    try:
        cur = con.cursor()        
        cur.execute("""INSERT INTO especialidades (nome,CID10_CAT) 
        VALUES (?,?)""",(nome,cid))
    except Exception as e:
        con.rollback()
        print("Ocorreu o erro ao adicinar especialidade:",e)
    else:
        con.commit()
        cur.close()

def AdicionarMedicos(nome,crm,con):
    try:
        cur = con.cursor()
        cur.execute("""INSERT INTO medicos (Nome,CRM)
        VALUES (?,?)""",(nome,crm,))
    except Exception as e:
        con.rollback()
        print("Ocorreu o erro ao adicinar medicos:",e)
    else:
        con.commit()
        cur.close()

def AdicionarMedicoespecialidades(med_id,esp_id,con):
    try:
        cur = con.cursor()
        cur.execute("""INSERT INTO medicos_especialidades (Medicos_ID,Especialidades_ID) 
        VALUES (?,?)""",(med_id,esp_id,))
    except Exception as e:
        con.rollback()
        print("Ocorreu o erro ao adicinar medico especialidade:",e)
    else:
        con.commit()
        cur.close()

def AdicionarServicosEspecialidades(serv_Id,esp_id,con):
    try:
        cur = con.cursor()
        cur.execute("""INSERT INTO servicos_especialidades (Servico_ID,Especialidades_ID)
        VALUES (?,?)""",(serv_Id,esp_id,))
    except Exception as e:
        con.rollback()
        print("Ocorreu o erro ao adicinar servico especialidade:",e)
    else:
        con.commit()
        cur.close()

def AdicionarFrequencias(serv_Id,sexo,Qtd_per,periodo_mes,Idade_min,Idade_max,con):
    try:
        cur = con.cursor()
        cur.execute("""INSERT INTO frequencias (Servico_ID,Sexo,QtdePeriodo,PeriodoMeses,IdadeMin,IdadeMax)
        VALUES (?,?,?,?,?,?)""",(serv_Id,sexo,Qtd_per,periodo_mes,Idade_min,Idade_max,))
    except Exception as e:
        con.rollback()
        print("Ocorreu o erro ao adicinar frequencias:",e)
    else:
        cur.close()

def LerTabela(con):
    try:
        cur = con.cursor()
        print('Atendimentos -------------------------')
        cur.execute("""SELECT * FROM atendimento; """)
        for linha in cur.fetchall():
            print(linha)
        print('pacientes --------------------')
        cur.execute("""SELECT * FROM pacientes; """)
        for linha in cur.fetchall():
            print(linha)
        print('atendimento servico -----------------------')
        cur.execute("""SELECT * FROM atendimento_servico; """)
        for linha in cur.fetchall():
            print(linha)
        print('servico-------------------------------------')
        cur.execute("""SELECT * FROM servico; """)
        for linha in cur.fetchall():
            print(linha)
        print('medicos_especialidades-------------------------------------')
        cur.execute("""SELECT * FROM medicos_especialidades; """)
        for linha in cur.fetchall():
            print(linha)
        print('medicos-------------------------------------')
        cur.execute("""SELECT * FROM medicos; """)
        for linha in cur.fetchall():
            print(linha)
        print('especialidades-------------------------------------')
        cur.execute("""SELECT * FROM especialidades; """)
        for linha in cur.fetchall():
            print(linha)
        print('servicos_especialidades-------------------------------------')
        cur.execute("""SELECT * FROM servicos_especialidades; """)
        for linha in cur.fetchall():
            print(linha)
        print('tipo_servicos-------------------------------------')
        cur.execute("""SELECT * FROM tipo_servico; """)
        for linha in cur.fetchall():
            print(linha)
        print('frequencias-------------------------------------')
        cur.execute("""SELECT * FROM frequencias; """)
        for linha in cur.fetchall():
            print(linha)
    except Exception as e:
        print("Ocorreu o erro ao ler tabela:",e)
    else:
        cur.close()

def ProduzirDados(con):
    try:
        DropTable(con)
        CriarAtendimento(con)
        CriarServico(con)
        CriarAtendimento_Servico(con)
        CriarPacientes(con)
        CriarMedicoEspecialidades(con)
        CriarMedicos(con)
        CriarEspecialidades(con)
        CriarServicosEspecialidades(con)
        CriarTipoServico(con)
        CriarFrequencias(con)
        AdicionarPacientes('Regis','000000000','00000000000','1995-09-01','M',con)
        AdicionarPacientes('Aloisio','111111111','11111111111','1935-10-05','M',con)
        AdicionarPacientes('Bruna','222222222','22222222222','2001-06-22','F',con)
        AdicionarPacientes('Wladi','333333333','33333333333','1966-12-03','M',con)
        AdicionarAtendimentos(1,'2015-05-22',80,1.75,'O paciente fez xyz',1,con)
        AdicionarAtendimentos(1,'2019-10-26',90,1.80,'O paciente fez xyz',2,con)
        AdicionarAtendimentos(2,'2015-10-06',75,1.77,'O paciente fez xyz',2,con)
        AdicionarAtendimentos(3,'2018-19-06',65,1.65,'O paciente fez xyz',5,con)
        AdicionarAtendimentos(3,'2019-10-26',75,1.70,'O paciente fez xyz',3,con)
        AdicionarAtendimentos(4,'2020-10-26',75,1.70,'O paciente fez xyz',3,con)
        AdicionarAtendimentos(4,'2019-10-26',75,1.70,'O paciente fez xyz',2,con)
        AdicionarAtendimentos(4,'2019-10-26',75,1.70,'O paciente fez xyz',1,con)
        AdicionarTipoServico('Exames de Diagnósticos Por Imagem',con)
        AdicionarTipoServico('Exames de Sangue',con)
        AdicionarTipoServico('Cirurgia de Alta Complexidade',con)
        AdicionarTipoServico('Consulta Médica',con)
        AdicionarTipoServico('Terapias',con)
        AdicionarServico(30310040,'Cirurgias fistulizantes com implantes valvulares',10,1,con)
        AdicionarServico(30213037,'Istmectomia ou nodulectomia - tireoide',100,2,con)
        AdicionarServico(30207045,'Redução de fratura de seio frontal',1000,3,con)
        AdicionarServico(30101875, 'Tratamento de escaras ou ulcerações com retalhos cutâneos locais',10000,3,con)
        AdicionarServico(20101074,'Avaliacao nutricional inclui consulta',500.50,5,con)
        AdicionarServico(30203023,'Tumor de lingua tratamento cirurgico',1500.25,1,con)
        AdicionarServico(30718015,'Amputacao ao nivel do braco tratamento cirurgico',1030.0,2,con)
        AdicionarEspecialidade('pediatria',1,con)
        AdicionarEspecialidade('Radiologia',2,con)
        AdicionarEspecialidade('Oncologia',3,con)
        AdicionarEspecialidade('Dermatologia',4,con)
        AdicionarMedicos('Walter',468754,con)
        AdicionarMedicos('Marcos',4463546,con)
        AdicionarMedicos('Roberto',45621676,con)
        AdicionarMedicos('Jandir Biroliro',6621615,con)
        AdicionarMedicoespecialidades(1,1,con)
        AdicionarMedicoespecialidades(1,2,con)
        AdicionarMedicoespecialidades(2,2,con)
        AdicionarMedicoespecialidades(3,3,con)
        AdicionarMedicoespecialidades(4,4,con)
        AdicionarServicosEspecialidades(1,1,con)
        AdicionarServicosEspecialidades(2,2,con)
        AdicionarServicosEspecialidades(3,3,con)
        AdicionarServicosEspecialidades(4,4,con)
        AdicionarServicosEspecialidades(5,1,con)
        AdicionarServicosEspecialidades(6,2,con)
        AdicionarFrequencias(1,'M',1,1500,15,30,con)
        AdicionarFrequencias(2,'M',1,30,1,10,con)
        AdicionarFrequencias(3,'A',1,1,0,120,con)
        AdicionarFrequencias(4,'F',1,10,30,80,con)
        AdicionarFrequencias(5,'F',1,50,12,30,con)
        AdicionarFrequencias(6,'A',1,0,100,900,con)
        AdicionarFrequencias(7,'M',1,5,5,120,con)
        AdicionarAtendimentos_servico(1,1,'2015-05-22',1,con) #m regis
        AdicionarAtendimentos_servico(2,1,'2015-05-22',1,con) #M regis
        AdicionarAtendimentos_servico(3,3,'2015-05-22',3,con) #m alosio ERRADO SEXO
        AdicionarAtendimentos_servico(4,6,'2019-05-22',1,con) #f bruna 
        AdicionarAtendimentos_servico(5,7,'2019-05-22',1,con) #f bruna ERRADO SEXO
        AdicionarAtendimentos_servico(7,5,'2015-05-22',2,con) #m wladi ERRADO ESP
        LerTabela(con)
    except Exception as e:
        print("Ocorreu o erro ao produzir dados:",e)


def criarVistaServicoSexo(con):
    try:
        cur = con.cursor()
        cur.execute("""DROP VIEW IF EXISTS Vista""")
        cur.execute("""CREATE VIEW Vista(SEXO,TIPO_SERVICO,VALOR,ID_MÉDICO,NOME_MÉDICO,ID_ATENDIMENTO,NOME_PACIENTE,SERVICO) as 
        SELECT p.sexo,t_serv.Tipo_servico,s.Valor,m.ID,m.Nome,at.ID,p.nome,s.Descricao
        FROM atendimento_servico at_serv INNER JOIN servico s ON at_serv.Servico_ID = s.ID
        INNER JOIN tipo_servico t_serv ON t_serv.ID = s.Tipo_servico_ID
        INNER JOIN atendimento at ON at.ID = at_serv.Atendimento_ID
        INNER JOIN pacientes p ON p.ID = at.Paciente_ID
        INNER JOIN medicos m ON m.ID = at_serv.Medicos_ID
        """)
    except Exception as e:
        print("Ocorreu um erro na hora de gerar a view de pacientes:",e)
    else:
        cur.close()
        return (pd.read_sql_query("SELECT * FROM Vista", con))




def main():
    try:
        con = sqlite3.connect("Hospital.db")
        # ProduzirDados(con)
        print(criarVistaServicoSexo(con))

    except Exception as e:
        print("Ocorreu o erro no final:",e)
    else:
        con.close()
        print("O programa encerrou -------------------------------")

main()



#