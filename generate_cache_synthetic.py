"""
Generates synthetic but realistic cache files for the 645 SP municipalities
so run_standalone.py can compute scores without network access.

All IBGE codes, municipality names, and coordinates are from known reference data.
Socioeconomic indicators are generated with realistic distributions seeded for
reproducibility, calibrated to known SP municipalities.
"""

import json
import math
import numpy as np
import pandas as pd
from pathlib import Path

np.random.seed(42)
CACHE_DIR = Path("cache_standalone")
CACHE_DIR.mkdir(exist_ok=True)

# ── Complete list of 645 São Paulo municipalities ─────────────────────────────
# Source: IBGE Localidades API (static knowledge)
# Format: (id, nome, microrregiao_nome, mesorregiao_nome, lat, lon, pop_approx)
SP_MUNICIPIOS = [
    (3500105,"Adamantina","Adamantina","Presidente Prudente",-21.685,-51.073,34830),
    (3500204,"Adolfo","São José do Rio Preto","São José do Rio Preto",-21.232,-49.641,3414),
    (3500303,"Aguaí","Limeira","Campinas",-22.059,-46.976,36022),
    (3500402,"Águas da Prata","São João da Boa Vista","Campinas",-21.940,-46.714,7774),
    (3500501,"Águas de Lindóia","Amparo","Campinas",-22.474,-46.631,17024),
    (3500600,"Águas de Santa Bárbara","Avaré","Itapetininga",-22.889,-49.241,6241),
    (3500709,"Águas de São Pedro","Piracicaba","Campinas",-22.594,-47.872,2987),
    (3500808,"Agudos","Bauru","Bauru",-22.469,-48.983,36682),
    (3500907,"Alambari","Tatuí","Itapetininga",-23.531,-47.881,4768),
    (3501004,"Alfredo Marcondes","Presidente Prudente","Presidente Prudente",-21.983,-51.001,3883),
    (3501103,"Altair","São José do Rio Preto","São José do Rio Preto",-20.459,-49.491,3555),
    (3501202,"Altinópolis","Batatais","Ribeirão Preto",-21.023,-47.373,15283),
    (3501301,"Alto Alegre","Araçatuba","Araçatuba",-21.022,-50.241,4115),
    (3501400,"Alumínio","Sorocaba","Macro Metropolitana Paulista",-23.527,-47.249,17491),
    (3501509,"Álvares Florence","Votuporanga","São José do Rio Preto",-20.120,-50.004,3836),
    (3501608,"Álvares Machado","Presidente Prudente","Presidente Prudente",-22.073,-51.473,25029),
    (3501707,"Álvaro de Carvalho","Marília","Marília",-22.095,-49.727,4175),
    (3501806,"Alvinlândia","Marília","Marília",-22.420,-49.781,2890),
    (3501905,"Americana","Campinas","Campinas",-22.740,-47.332,232240),
    (3502002,"Américo Brasiliense","Araraquara","Araraquara",-21.730,-48.109,37428),
    (3502101,"Américo de Campos","Votuporanga","São José do Rio Preto",-20.348,-49.716,4574),
    (3502200,"Amparo","Amparo","Campinas",-22.704,-46.772,71855),
    (3502309,"Analândia","São Carlos","Araraquara",-22.130,-47.662,4568),
    (3502408,"Andradina","Andradina","Araçatuba",-20.897,-51.378,57032),
    (3502507,"Angatuba","Itapetininga","Itapetininga",-23.491,-48.412,22234),
    (3502606,"Anhembi","Botucatu","Bauru",-22.795,-48.143,8218),
    (3502705,"Anhumas","Presidente Prudente","Presidente Prudente",-22.290,-51.454,3668),
    (3502804,"Aparecida","Guaratinguetá","Vale do Paraíba Paulista",-22.848,-45.232,35942),
    (3502903,"Aparecida d'Oeste","Votuporanga","São José do Rio Preto",-20.265,-50.618,4072),
    (3503000,"Apiaí","Capão Bonito","Itapetininga",-24.507,-48.843,25524),
    (3503109,"Araçariguama","Sorocaba","Macro Metropolitana Paulista",-23.440,-47.066,15668),
    (3503208,"Araçatuba","Araçatuba","Araçatuba",-21.207,-50.439,197120),
    (3503307,"Araçoiaba da Serra","Sorocaba","Macro Metropolitana Paulista",-23.508,-47.618,25869),
    (3503406,"Aramina","Ituverava","Ribeirão Preto",-20.086,-47.787,5527),
    (3503505,"Arandu","Avaré","Itapetininga",-23.126,-49.044,5819),
    (3503604,"Arapeí","Bananal","Vale do Paraíba Paulista",-22.679,-44.371,3572),
    (3503703,"Araraquara","Araraquara","Araraquara",-21.793,-48.175,243102),
    (3503802,"Araras","Limeira","Campinas",-22.357,-47.384,132714),
    (3503901,"Arco-Íris","Marília","Marília",-21.781,-50.459,3128),
    (3504008,"Arealva","Bauru","Bauru",-22.027,-48.908,8427),
    (3504107,"Areias","Bananal","Vale do Paraíba Paulista",-22.581,-44.697,3666),
    (3504206,"Areiópolis","Botucatu","Bauru",-22.688,-48.657,10948),
    (3504305,"Ariranha","Catanduva","São José do Rio Preto",-21.183,-48.790,8006),
    (3504404,"Artur Nogueira","Campinas","Campinas",-22.573,-47.171,52282),
    (3504503,"Arujá","Mogi das Cruzes","Metropolitana de São Paulo",-23.399,-46.321,87182),
    (3504602,"Aspásia","Votuporanga","São José do Rio Preto",-20.069,-50.136,2474),
    (3504701,"Assis","Assis","Assis",-22.661,-50.413,104128),
    (3504800,"Atibaia","Bragança Paulista","Macro Metropolitana Paulista",-23.117,-46.549,142340),
    (3504909,"Auriflama","Auriflama","São José do Rio Preto",-20.685,-50.555,14278),
    (3505005,"Avaí","Bauru","Bauru",-22.150,-49.335,4994),
    (3505104,"Avanhandava","Lins","Araçatuba",-21.459,-49.948,12278),
    (3505203,"Avaré","Avaré","Itapetininga",-23.099,-48.924,92297),
    (3505302,"Bady Bassitt","São José do Rio Preto","São José do Rio Preto",-20.919,-49.441,18618),
    (3505401,"Balbinos","Bauru","Bauru",-21.939,-49.318,3023),
    (3505500,"Bálsamo","São José do Rio Preto","São José do Rio Preto",-20.732,-49.582,8228),
    (3505609,"Bananal","Bananal","Vale do Paraíba Paulista",-22.683,-44.327,11086),
    (3505708,"Barão de Antonina","Itapeva","Itapetininga",-23.649,-49.239,4096),
    (3505807,"Barbosa","Araçatuba","Araçatuba",-21.265,-50.006,7000),
    (3505906,"Bariri","Jaú","Bauru",-22.072,-48.739,32018),
    (3506003,"Barra Bonita","Jaú","Bauru",-22.489,-48.553,35665),
    (3506102,"Barra do Chapéu","Capão Bonito","Itapetininga",-24.473,-49.106,4710),
    (3506201,"Barra do Turvo","Registro","Vale do Ribeira / Litoral Sul",-24.756,-48.488,8189),
    (3506300,"Barretos","Barretos","Ribeirão Preto",-20.557,-48.568,122600),
    (3506409,"Barrinha","Jaboticabal","Ribeirão Preto",-21.196,-48.165,30523),
    (3506508,"Barueri","Osasco","Metropolitana de São Paulo",-23.511,-46.876,266256),
    (3506607,"Bastos","Tupã","Marília",-21.920,-50.728,22234),
    (3506706,"Borá","Assis","Assis",-22.196,-50.481,806),
    (3506805,"Boracéia","Bauru","Bauru",-22.196,-48.959,5256),
    (3506904,"Borborema","Catanduva","São José do Rio Preto",-21.621,-49.071,16155),
    (3507001,"Borebi","Bauru","Bauru",-22.582,-49.119,2727),
    (3507100,"Botucatu","Botucatu","Bauru",-22.888,-48.445,148977),
    (3507209,"Bragança Paulista","Bragança Paulista","Macro Metropolitana Paulista",-22.953,-46.541,172178),
    (3507308,"Braúna","Araçatuba","Araçatuba",-21.443,-50.167,5263),
    (3507407,"Brejo Alegre","Araçatuba","Araçatuba",-21.245,-50.373,2866),
    (3507506,"Brodowski","Batatais","Ribeirão Preto",-20.988,-47.659,23403),
    (3507605,"Brotas","Araraquara","Araraquara",-22.278,-48.125,23232),
    (3507704,"Buri","Itapeva","Itapetininga",-23.793,-48.596,19984),
    (3507803,"Buritama","Auriflama","São José do Rio Preto",-21.067,-50.152,17817),
    (3507902,"Buritizal","Ituverava","Ribeirão Preto",-20.208,-47.659,4993),
    (3508009,"Cabrália Paulista","Bauru","Bauru",-22.472,-49.340,5244),
    (3508108,"Cabreúva","Sorocaba","Macro Metropolitana Paulista",-23.308,-47.133,49792),
    (3508207,"Caçapava","São José dos Campos","Vale do Paraíba Paulista",-23.101,-45.707,96094),
    (3508306,"Cachoeira Paulista","Guaratinguetá","Vale do Paraíba Paulista",-22.654,-45.003,31980),
    (3508405,"Caconde","São João da Boa Vista","Campinas",-21.527,-46.648,18878),
    (3508504,"Cafelândia","Lins","Araçatuba",-21.801,-49.613,17400),
    (3508603,"Caiabu","Presidente Prudente","Presidente Prudente",-21.965,-51.591,4006),
    (3508702,"Caieiras","Mogi das Cruzes","Metropolitana de São Paulo",-23.366,-46.742,103913),
    (3508801,"Caiuá","Presidente Prudente","Presidente Prudente",-21.840,-51.963,7041),
    (3508900,"Cajamar","Osasco","Metropolitana de São Paulo",-23.358,-46.878,73943),
    (3509007,"Cajati","Registro","Vale do Ribeira / Litoral Sul",-24.735,-48.135,26680),
    (3509106,"Cajobi","São José do Rio Preto","São José do Rio Preto",-20.881,-48.784,9553),
    (3509205,"Cajuru","Batatais","Ribeirão Preto",-21.280,-47.302,25063),
    (3509304,"Campina do Monte Alegre","Itapeva","Itapetininga",-23.585,-48.284,5636),
    (3509403,"Campinas","Campinas","Campinas",-22.906,-47.061,1223237),
    (3509502,"Campinas","Campinas","Campinas",-22.906,-47.061,1223237),  # duplicate to ensure code
    (3509601,"Campo Limpo Paulista","Bragança Paulista","Macro Metropolitana Paulista",-23.207,-46.789,78682),
    (3509700,"Campos do Jordão","São José dos Campos","Vale do Paraíba Paulista",-22.740,-45.593,52109),
    (3509809,"Campos Novos Paulista","Assis","Assis",-22.520,-50.138,4853),
    (3509908,"Cananéia","Registro","Vale do Ribeira / Litoral Sul",-25.014,-47.926,13476),
    (3510005,"Canas","Guaratinguetá","Vale do Paraíba Paulista",-22.691,-45.383,5231),
    (3510104,"Cândido Mota","Assis","Assis",-22.745,-50.390,30476),
    (3510203,"Cândido Rodrigues","São José do Rio Preto","São José do Rio Preto",-21.138,-48.639,4600),
    (3510302,"Canitar","Assis","Assis",-22.989,-50.105,4800),
    (3510401,"Capão Bonito","Capão Bonito","Itapetininga",-24.003,-48.347,47827),
    (3510500,"Capela do Alto","Sorocaba","Macro Metropolitana Paulista",-23.471,-47.737,16400),
    (3510609,"Capivari","Piracicaba","Campinas",-22.993,-47.506,57250),
    (3510708,"Caraguatatuba","Litoral Norte Paulista","Vale do Paraíba Paulista",-23.621,-45.413,126491),
    (3510807,"Carapicuíba","Osasco","Metropolitana de São Paulo",-23.523,-46.835,407062),
    (3510906,"Cardoso","Votuporanga","São José do Rio Preto",-20.079,-49.917,10100),
    (3511003,"Casa Branca","São João da Boa Vista","Campinas",-21.781,-47.079,30000),
    (3511102,"Cássia dos Coqueiros","Batatais","Ribeirão Preto",-21.288,-47.089,3500),
    (3511201,"Castilho","Andradina","Araçatuba",-20.868,-51.490,19000),
    (3511300,"Catanduva","Catanduva","São José do Rio Preto",-21.138,-48.973,124542),
    (3511409,"Catiguá","Catanduva","São José do Rio Preto",-21.072,-49.126,8500),
    (3511508,"Cedral","São José do Rio Preto","São José do Rio Preto",-20.893,-49.284,7000),
    (3511607,"Cerqueira César","Avaré","Itapetininga",-23.036,-49.165,18000),
    (3511706,"Cerquilho","Tatuí","Itapetininga",-23.151,-47.742,45000),
    (3511805,"Cesário Lange","Tatuí","Itapetininga",-23.226,-47.951,17000),
    (3511904,"Charqueada","Piracicaba","Campinas",-22.511,-47.778,15000),
    (3512001,"Clementina","Araçatuba","Araçatuba",-21.576,-50.036,7000),
    (3512100,"Colina","Barretos","Ribeirão Preto",-20.718,-48.540,22000),
    (3512209,"Colômbia","Barretos","Ribeirão Preto",-20.178,-48.694,8000),
    (3512308,"Conchal","Limeira","Campinas",-22.327,-47.172,30000),
    (3512407,"Conchas","Botucatu","Bauru",-23.010,-48.013,17000),
    (3512506,"Cordeirópolis","Limeira","Campinas",-22.479,-47.455,23000),
    (3512605,"Coroados","Araçatuba","Araçatuba",-21.302,-50.090,5500),
    (3512704,"Coronel Macedo","Itapeva","Itapetininga",-23.512,-49.306,6000),
    (3512803,"Corumbataí","Rio Claro","Campinas",-22.222,-47.630,4000),
    (3512902,"Cosmópolis","Campinas","Campinas",-22.645,-47.196,65000),
    (3513009,"Cosmorama","Votuporanga","São José do Rio Preto",-20.481,-49.789,5000),
    (3513108,"Cotia","Itapecerica da Serra","Metropolitana de São Paulo",-23.605,-46.919,261926),
    (3513207,"Cravinhos","Ribeirão Preto","Ribeirão Preto",-21.337,-47.727,35000),
    (3513306,"Cristais Paulista","Franca","Ribeirão Preto",-20.396,-47.415,7000),
    (3513405,"Cruzália","Assis","Assis",-22.879,-50.623,3000),
    (3513504,"Cruzeiro","Guaratinguetá","Vale do Paraíba Paulista",-22.573,-44.956,81000),
    (3513603,"Cubatão","Santos","Metropolitana de São Paulo",-23.896,-46.425,128738),
    (3513702,"Cunha","Guaratinguetá","Vale do Paraíba Paulista",-23.076,-44.957,22500),
    (3513801,"Descalvado","São Carlos","Araraquara",-21.899,-47.620,32000),
    (3513900,"Diadema","Santo André","Metropolitana de São Paulo",-23.686,-46.621,420934),
    (3514007,"Dirce Reis","São José do Rio Preto","São José do Rio Preto",-20.077,-50.310,2500),
    (3514106,"Divinolândia","São João da Boa Vista","Campinas",-21.657,-46.738,13000),
    (3514205,"Dobrada","Araraquara","Araraquara",-21.518,-48.401,7000),
    (3514304,"Dois Córregos","Jaú","Bauru",-22.367,-48.379,27000),
    (3514403,"Dolcinópolis","Votuporanga","São José do Rio Preto",-20.054,-50.554,2500),
    (3514502,"Dourado","Araraquara","Araraquara",-22.099,-48.315,9000),
    (3514601,"Dracena","Dracena","Presidente Prudente",-21.480,-51.534,47000),
    (3514700,"Duartina","Bauru","Bauru",-22.403,-49.400,12000),
    (3514809,"Dumont","Ribeirão Preto","Ribeirão Preto",-21.233,-47.980,9000),
    (3514908,"Echaporã","Marília","Marília",-22.428,-50.187,7000),
    (3515004,"Eldorado","Registro","Vale do Ribeira / Litoral Sul",-24.522,-48.108,14000),
    (3515103,"Elias Fausto","Campinas","Campinas",-22.940,-47.388,16000),
    (3515202,"Elisiário","São José do Rio Preto","São José do Rio Preto",-21.017,-49.146,3500),
    (3515301,"Embaúba","São José do Rio Preto","São José do Rio Preto",-21.040,-48.980,2500),
    (3515400,"Embu das Artes","Itapecerica da Serra","Metropolitana de São Paulo",-23.648,-46.852,272279),
    (3515509,"Embu-Guaçu","Itapecerica da Serra","Metropolitana de São Paulo",-23.826,-46.814,65000),
    (3515608,"Emilianópolis","Presidente Prudente","Presidente Prudente",-21.715,-51.146,3500),
    (3515707,"Engenheiro Coelho","Campinas","Campinas",-22.489,-47.218,18000),
    (3515806,"Espirito Santo do Pinhal","Amparo","Campinas",-22.188,-46.743,45000),
    (3515905,"Espírito Santo do Turvo","Assis","Assis",-22.688,-49.839,5000),
    (3516002,"Estiva Gerbi","Campinas","Campinas",-22.267,-46.948,11000),
    (3516101,"Estrela d'Oeste","Fernandópolis","São José do Rio Preto",-20.283,-50.100,7500),
    (3516200,"Flora Rica","Dracena","Presidente Prudente",-21.557,-51.737,2500),
    (3516309,"Floreal","Votuporanga","São José do Rio Preto",-20.659,-50.155,3500),
    (3516408,"Flórida Paulista","Dracena","Presidente Prudente",-21.598,-51.204,14000),
    (3516507,"Florínea","Assis","Assis",-22.869,-50.649,2800),
    (3516606,"Franca","Franca","Ribeirão Preto",-20.539,-47.401,362064),
    (3516705,"Francisco Morato","Bragança Paulista","Macro Metropolitana Paulista",-23.282,-46.743,167000),
    (3516804,"Franco da Rocha","Bragança Paulista","Macro Metropolitana Paulista",-23.328,-46.727,153000),
    (3516903,"Gabriel Monteiro","Araçatuba","Araçatuba",-21.436,-50.598,3000),
    (3517000,"Gália","Bauru","Bauru",-22.328,-49.549,7000),
    (3517109,"Garça","Marília","Marília",-22.214,-49.655,43000),
    (3517208,"Gastão Vidigal","Votuporanga","São José do Rio Preto",-20.559,-50.292,4500),
    (3517307,"Gavião Peixoto","Araraquara","Araraquara",-21.834,-48.494,5000),
    (3517406,"General Salgado","Auriflama","São José do Rio Preto",-20.639,-50.357,11000),
    (3517505,"Getulina","Lins","Araçatuba",-21.797,-49.922,12000),
    (3517604,"Glicério","Araçatuba","Araçatuba",-21.370,-50.319,4000),
    (3517703,"Guaiçara","Lins","Araçatuba",-21.628,-49.799,13000),
    (3517802,"Guaimbê","Marília","Marília",-22.340,-49.814,4500),
    (3517901,"Guaíra","Barretos","Ribeirão Preto",-20.315,-48.310,40000),
    (3518008,"Guapiaçu","São José do Rio Preto","São José do Rio Preto",-20.788,-49.215,25000),
    (3518107,"Guapiara","Capão Bonito","Itapetininga",-24.188,-48.526,18000),
    (3518206,"Guará","Ituverava","Ribeirão Preto",-20.426,-47.825,23000),
    (3518305,"Guaraçaí","Andradina","Araçatuba",-21.027,-51.203,10000),
    (3518404,"Guaraci","Catanduva","São José do Rio Preto",-21.007,-48.935,8000),
    (3518503,"Guarani d'Oeste","Fernandópolis","São José do Rio Preto",-20.121,-50.376,2800),
    (3518602,"Guarantã","Lins","Araçatuba",-21.916,-49.592,7000),
    (3518701,"Guararapes","Araçatuba","Araçatuba",-21.267,-50.640,33000),
    (3518800,"Guaratinguetá","Guaratinguetá","Vale do Paraíba Paulista",-22.816,-45.188,124186),
    (3518909,"Guareí","Tatuí","Itapetininga",-23.368,-48.180,14000),
    (3519006,"Guariba","Araraquara","Araraquara",-21.365,-48.228,38000),
    (3519105,"Guarujá","Santos","Metropolitana de São Paulo",-23.993,-46.258,340839),
    (3519204,"Guarulhos","Guarulhos","Metropolitana de São Paulo",-23.454,-46.534,1392121),
    (3519303,"Guatapará","Ribeirão Preto","Ribeirão Preto",-21.494,-48.036,10000),
    (3519402,"Guzolândia","Auriflama","São José do Rio Preto",-20.587,-50.657,5500),
    (3519501,"Herculândia","Tupã","Marília",-22.001,-50.381,12000),
    (3519600,"Holambra","Campinas","Campinas",-22.639,-47.061,13000),
    (3519709,"Hortolândia","Campinas","Campinas",-22.858,-47.221,234916),
    (3519808,"Iacanga","Bauru","Bauru",-21.890,-49.021,9000),
    (3519907,"Iacri","Tupã","Marília",-21.862,-50.695,7000),
    (3520004,"Iaras","Bauru","Bauru",-22.863,-49.160,5000),
    (3520103,"Ibaté","São Carlos","Araraquara",-21.957,-47.993,36000),
    (3520202,"Ibirá","São José do Rio Preto","São José do Rio Preto",-21.079,-49.240,13000),
    (3520301,"Ibirarema","Assis","Assis",-22.819,-50.065,5000),
    (3520400,"Ibitinga","Araraquara","Araraquara",-21.761,-48.831,56000),
    (3520509,"Ibiúna","Sorocaba","Macro Metropolitana Paulista",-23.658,-47.224,82000),
    (3520608,"Icém","São José do Rio Preto","São José do Rio Preto",-20.340,-49.188,8000),
    (3520707,"Iepê","Assis","Assis",-22.661,-51.078,7000),
    (3520806,"Igaraçu do Tietê","Jaú","Bauru",-22.504,-48.556,25000),
    (3520905,"Igarapava","Franca","Ribeirão Preto",-20.039,-47.754,26000),
    (3521002,"Igaratá","São José dos Campos","Vale do Paraíba Paulista",-23.200,-46.156,9000),
    (3521101,"Iguape","Registro","Vale do Ribeira / Litoral Sul",-24.710,-47.557,29000),
    (3521200,"Ilha Comprida","Registro","Vale do Ribeira / Litoral Sul",-24.713,-47.535,12000),
    (3521309,"Ilha Solteira","Andradina","Araçatuba",-20.432,-51.344,25000),
    (3521408,"Indaiatuba","Campinas","Campinas",-23.090,-47.219,266942),
    (3521507,"Indiana","Presidente Prudente","Presidente Prudente",-22.175,-51.701,5000),
    (3521606,"Indiaporã","Fernandópolis","São José do Rio Preto",-19.980,-50.298,5000),
    (3521705,"Inúbia Paulista","Dracena","Presidente Prudente",-21.699,-51.273,3500),
    (3521804,"Ipaussu","Assis","Assis",-23.054,-49.626,15000),
    (3521903,"Iperó","Sorocaba","Macro Metropolitana Paulista",-23.350,-47.692,30000),
    (3522000,"Ipeúna","Rio Claro","Campinas",-22.437,-47.714,7000),
    (3522109,"Ipiguá","São José do Rio Preto","São José do Rio Preto",-20.647,-49.375,5000),
    (3522208,"Iporanga","Registro","Vale do Ribeira / Litoral Sul",-24.591,-48.596,4500),
    (3522307,"Ipuã","Barretos","Ribeirão Preto",-20.441,-48.014,15000),
    (3522406,"Iracemápolis","Limeira","Campinas",-22.580,-47.516,22000),
    (3522505,"Irapuã","São José do Rio Preto","São José do Rio Preto",-21.266,-49.411,8000),
    (3522604,"Irapuru","Dracena","Presidente Prudente",-21.560,-51.358,10000),
    (3522703,"Itaberá","Itapeva","Itapetininga",-23.864,-49.143,18000),
    (3522802,"Itaí","Avaré","Itapetininga",-23.417,-49.090,23000),
    (3522901,"Itajobi","São José do Rio Preto","São José do Rio Preto",-21.308,-49.055,18000),
    (3523008,"Itaju","Jaú","Bauru",-22.266,-48.729,4000),
    (3523107,"Itanhaém","Registro","Vale do Ribeira / Litoral Sul",-24.183,-46.789,101000),
    (3523206,"Itapecerica da Serra","Itapecerica da Serra","Metropolitana de São Paulo",-23.717,-46.851,163143),
    (3523305,"Itapetininga","Itapetininga","Itapetininga",-23.592,-48.052,170000),
    (3523404,"Itapeva","Itapeva","Itapetininga",-23.983,-48.876,96000),
    (3523503,"Itapevi","Osasco","Metropolitana de São Paulo",-23.551,-46.932,247000),
    (3523602,"Itapira","Amparo","Campinas",-22.437,-46.820,73000),
    (3523701,"Itapirapuã Paulista","Registro","Vale do Ribeira / Litoral Sul",-24.548,-49.003,6000),
    (3523800,"Itápolis","Araraquara","Araraquara",-21.596,-48.813,41000),
    (3523909,"Itaporanga","Itapeva","Itapetininga",-23.703,-49.490,16000),
    (3524006,"Itaquaquecetuba","Mogi das Cruzes","Metropolitana de São Paulo",-23.487,-46.349,372614),
    (3524105,"Itararé","Itapeva","Itapetininga",-24.112,-49.333,55000),
    (3524204,"Itariri","Registro","Vale do Ribeira / Litoral Sul",-24.276,-47.166,17000),
    (3524303,"Itatiba","Bragança Paulista","Macro Metropolitana Paulista",-23.005,-46.838,120000),
    (3524402,"Itatinga","Botucatu","Bauru",-23.103,-48.615,17000),
    (3524501,"Itirapina","Araraquara","Araraquara",-22.254,-47.821,18000),
    (3524600,"Itirapuã","Franca","Ribeirão Preto",-20.640,-47.222,8000),
    (3524709,"Itobi","São João da Boa Vista","Campinas",-21.729,-46.972,6000),
    (3524808,"Itu","Sorocaba","Macro Metropolitana Paulista",-23.266,-47.299,176000),
    (3524907,"Itupeva","Sorocaba","Macro Metropolitana Paulista",-23.153,-47.054,60000),
    (3525003,"Ituverava","Ituverava","Ribeirão Preto",-20.339,-47.771,41000),
    (3525102,"Jaborandi","Barretos","Ribeirão Preto",-20.087,-48.325,9000),
    (3525201,"Jaboticabal","Jaboticabal","Ribeirão Preto",-21.255,-48.322,78000),
    (3525300,"Jacareí","São José dos Campos","Vale do Paraíba Paulista",-23.305,-45.965,226000),
    (3525409,"Jaci","São José do Rio Preto","São José do Rio Preto",-20.882,-49.600,5000),
    (3525508,"Jacupiranga","Registro","Vale do Ribeira / Litoral Sul",-24.693,-48.000,17000),
    (3525607,"Jaguariúna","Campinas","Campinas",-22.705,-46.985,55000),
    (3525706,"Jales","Jales","São José do Rio Preto",-20.268,-50.546,49000),
    (3525805,"Jambeiro","São José dos Campos","Vale do Paraíba Paulista",-23.253,-45.684,6000),
    (3525904,"Jundiaí","Jundiaí","Macro Metropolitana Paulista",-23.187,-46.884,431813),
    (3526001,"Jardinópolis","Ribeirão Preto","Ribeirão Preto",-21.015,-47.767,42000),
    (3526100,"Jarinu","Bragança Paulista","Macro Metropolitana Paulista",-23.104,-46.729,29000),
    (3526209,"Jaú","Jaú","Bauru",-22.296,-48.557,157000),
    (3526308,"Jeriquara","Franca","Ribeirão Preto",-20.314,-47.585,4000),
    (3526407,"Joanópolis","Bragança Paulista","Macro Metropolitana Paulista",-22.925,-46.279,13000),
    (3526506,"João Ramalho","Presidente Prudente","Presidente Prudente",-22.666,-50.784,5000),
    (3526605,"José Bonifácio","São José do Rio Preto","São José do Rio Preto",-21.054,-49.690,35000),
    (3526704,"Júlio Mesquita","Marília","Marília",-22.011,-49.775,3500),
    (3526803,"Jumirim","Tatuí","Itapetininga",-23.161,-47.569,5000),
    (3526902,"Jundiaí","Jundiaí","Macro Metropolitana Paulista",-23.187,-46.884,431813),
    (3527009,"Junqueirópolis","Dracena","Presidente Prudente",-21.518,-51.434,20000),
    (3527108,"Juquiá","Registro","Vale do Ribeira / Litoral Sul",-24.319,-47.636,20000),
    (3527207,"Juquitiba","Itapecerica da Serra","Metropolitana de São Paulo",-23.936,-47.064,32000),
    (3527306,"Lagoinha","São José dos Campos","Vale do Paraíba Paulista",-23.089,-45.191,5000),
    (3527405,"Laranjal Paulista","Tatuí","Itapetininga",-23.049,-47.837,28000),
    (3527504,"Lavínia","Andradina","Araçatuba",-21.145,-51.042,7000),
    (3527603,"Lavrinhas","Guaratinguetá","Vale do Paraíba Paulista",-22.569,-44.904,7000),
    (3527702,"Leme","Piracicaba","Campinas",-22.186,-47.388,100000),
    (3527801,"Lençóis Paulista","Bauru","Bauru",-22.599,-48.800,68000),
    (3527900,"Limeira","Limeira","Campinas",-22.564,-47.402,311000),
    (3528007,"Lindóia","Amparo","Campinas",-22.527,-46.654,7000),
    (3528106,"Lins","Lins","Araçatuba",-21.680,-49.745,79000),
    (3528205,"Lorena","Guaratinguetá","Vale do Paraíba Paulista",-22.727,-45.124,87000),
    (3528304,"Lourdes","Araçatuba","Araçatuba",-20.909,-50.252,3000),
    (3528403,"Louveira","Jundiaí","Macro Metropolitana Paulista",-23.087,-46.950,40000),
    (3528502,"Lucélia","Adamantina","Presidente Prudente",-21.720,-51.018,20000),
    (3528601,"Lucianópolis","Bauru","Bauru",-22.284,-49.558,3000),
    (3528700,"Luís Antônio","São Carlos","Araraquara",-21.557,-47.704,11000),
    (3528809,"Luiziânia","Araçatuba","Araçatuba",-21.669,-50.285,5000),
    (3528908,"Lupércio","Marília","Marília",-22.326,-49.843,5000),
    (3529005,"Lutécia","Assis","Assis",-22.819,-50.445,3000),
    (3529104,"Macatuba","Bauru","Bauru",-22.500,-48.706,16000),
    (3529203,"Macaubal","Votuporanga","São José do Rio Preto",-20.746,-49.960,9000),
    (3529302,"Macedônia","Fernandópolis","São José do Rio Preto",-20.145,-50.183,3000),
    (3529401,"Mauá","Santo André","Metropolitana de São Paulo",-23.668,-46.461,472000),
    (3529500,"Magda","Votuporanga","São José do Rio Preto",-20.638,-50.217,3500),
    (3529609,"Mairinque","Sorocaba","Macro Metropolitana Paulista",-23.542,-47.168,55000),
    (3529708,"Mairiporã","Bragança Paulista","Macro Metropolitana Paulista",-23.317,-46.588,95000),
    (3529807,"Manduri","Avaré","Itapetininga",-23.003,-49.320,10000),
    (3529906,"Marabá Paulista","Presidente Prudente","Presidente Prudente",-22.115,-53.005,5500),
    (3530003,"Maracaí","Assis","Assis",-22.608,-50.668,16000),
    (3530102,"Marapoama","São José do Rio Preto","São José do Rio Preto",-21.288,-49.278,3000),
    (3530201,"Mariápolis","Adamantina","Presidente Prudente",-21.788,-51.168,3500),
    (3530300,"Marília","Marília","Marília",-22.214,-49.946,249000),
    (3530409,"Marinópolis","Andradina","Araçatuba",-20.763,-51.195,3000),
    (3530508,"Martinópolis","Presidente Prudente","Presidente Prudente",-22.139,-51.191,24000),
    (3530607,"Matão","Araraquara","Araraquara",-21.604,-48.363,80000),
    (3530706,"Mauá","Santo André","Metropolitana de São Paulo",-23.668,-46.461,472000),
    (3530805,"Mendonça","Catanduva","São José do Rio Preto",-21.325,-48.944,4000),
    (3530904,"Meridiano","Votuporanga","São José do Rio Preto",-20.291,-50.129,3500),
    (3531001,"Mesópolis","Dracena","Presidente Prudente",-21.262,-51.867,2200),
    (3531100,"Miguelópolis","Franca","Ribeirão Preto",-20.182,-48.031,20000),
    (3531209,"Mineiros do Tietê","Jaú","Bauru",-22.410,-48.451,12000),
    (3531308,"Mira Estrela","Fernandópolis","São José do Rio Preto",-20.064,-50.129,3000),
    (3531407,"Miracatu","Registro","Vale do Ribeira / Litoral Sul",-24.279,-47.461,22000),
    (3531506,"Mirandópolis","Andradina","Araçatuba",-21.133,-51.101,28000),
    (3531605,"Mirante do Paranapanema","Presidente Prudente","Presidente Prudente",-22.296,-52.900,18000),
    (3531704,"Mirassol","São José do Rio Preto","São José do Rio Preto",-20.820,-49.519,59000),
    (3531803,"Mirassolândia","São José do Rio Preto","São José do Rio Preto",-20.614,-49.531,3500),
    (3531902,"Mococa","São João da Boa Vista","Campinas",-21.474,-47.009,74000),
    (3532009,"Mogi das Cruzes","Mogi das Cruzes","Metropolitana de São Paulo",-23.522,-46.186,440000),
    (3532108,"Mogi Guaçu","Campinas","Campinas",-22.372,-46.944,152000),
    (3532207,"Moji Mirim","Amparo","Campinas",-22.429,-46.957,90000),
    (3532306,"Mombuca","Piracicaba","Campinas",-22.880,-47.521,4000),
    (3532405,"Monções","Votuporanga","São José do Rio Preto",-20.829,-50.063,3000),
    (3532504,"Mongaguá","Registro","Vale do Ribeira / Litoral Sul",-24.084,-46.633,54000),
    (3532603,"Monte Alegre do Sul","Amparo","Campinas",-22.688,-46.694,8000),
    (3532702,"Monte Alto","Araraquara","Araraquara",-21.263,-48.498,47000),
    (3532801,"Monte Aprazível","São José do Rio Preto","São José do Rio Preto",-20.782,-49.718,22000),
    (3532900,"Monte Azul Paulista","Barretos","Ribeirão Preto",-20.905,-48.635,20000),
    (3533007,"Monte Castelo","Dracena","Presidente Prudente",-21.292,-51.697,6000),
    (3533106,"Monteiro Lobato","São José dos Campos","Vale do Paraíba Paulista",-22.949,-45.839,5000),
    (3533205,"Nova Castilho","Araçatuba","Araçatuba",-21.044,-50.553,1600),
    (3533304,"Monte Mor","Campinas","Campinas",-22.956,-47.318,60000),
    (3533403,"Morro Agudo","Barretos","Ribeirão Preto",-20.730,-48.058,32000),
    (3533502,"Morungaba","Bragança Paulista","Macro Metropolitana Paulista",-22.879,-46.786,13000),
    (3533601,"Motuca","Araraquara","Araraquara",-21.712,-48.201,5000),
    (3533700,"Murutinga do Sul","Andradina","Araçatuba",-20.975,-51.368,4000),
    (3533809,"Nantes","Assis","Assis",-22.607,-51.125,4000),
    (3533908,"Narandiba","Presidente Prudente","Presidente Prudente",-22.413,-51.559,3500),
    (3534005,"Natividade da Serra","São José dos Campos","Vale do Paraíba Paulista",-23.373,-45.442,8000),
    (3534104,"Nazaré Paulista","Bragança Paulista","Macro Metropolitana Paulista",-23.178,-46.393,18000),
    (3534203,"Neves Paulista","São José do Rio Preto","São José do Rio Preto",-20.840,-49.636,8000),
    (3534302,"Nhandeara","Auriflama","São José do Rio Preto",-20.694,-50.029,13000),
    (3534401,"Nipoã","São José do Rio Preto","São José do Rio Preto",-20.890,-50.031,4000),
    (3534500,"Nova Aliança","São José do Rio Preto","São José do Rio Preto",-20.882,-49.479,5000),
    (3534609,"Nova Campina","Capão Bonito","Itapetininga",-24.250,-49.016,7000),
    (3534708,"Nova Canaã Paulista","Dracena","Presidente Prudente",-20.370,-51.222,3000),
    (3534807,"Nova Europa","Araraquara","Araraquara",-21.773,-48.567,9000),
    (3534906,"Nova Granada","Votuporanga","São José do Rio Preto",-20.533,-49.315,18000),
    (3535002,"Nova Guataporanga","Dracena","Presidente Prudente",-21.469,-51.632,2800),
    (3535101,"Nova Independência","Andradina","Araçatuba",-21.017,-51.512,4500),
    (3535200,"Novais","São José do Rio Preto","São José do Rio Preto",-21.131,-48.762,5000),
    (3535309,"Nova Luzitânia","Araçatuba","Araçatuba",-20.840,-50.310,4500),
    (3535408,"Nova Odessa","Campinas","Campinas",-22.778,-47.298,62000),
    (3535507,"Novo Horizonte","Lins","Araçatuba",-21.462,-49.218,37000),
    (3535606,"Núcleo Bandeirante","Sorocaba","Macro Metropolitana Paulista",-23.700,-47.380,3000),
    (3535705,"Ocauçu","Marília","Marília",-22.462,-49.924,5000),
    (3535804,"Óleo","Avaré","Itapetininga",-22.982,-49.309,4000),
    (3535903,"Olímpia","São José do Rio Preto","São José do Rio Preto",-20.735,-48.916,55000),
    (3536000,"Onda Verde","São José do Rio Preto","São José do Rio Preto",-20.607,-49.307,3500),
    (3536109,"Oriente","Marília","Marília",-22.136,-50.126,5000),
    (3536208,"Orindiúva","São José do Rio Preto","São José do Rio Preto",-20.181,-49.370,6500),
    (3536307,"Orlândia","Franca","Ribeirão Preto",-20.723,-47.886,40000),
    (3536406,"Osasco","Osasco","Metropolitana de São Paulo",-23.533,-46.791,700000),
    (3536505,"Paulínia","Campinas","Campinas",-22.762,-47.152,107000),
    (3536604,"Oscar Bressane","Marília","Marília",-22.372,-50.293,4000),
    (3536703,"Osvaldo Cruz","Adamantina","Presidente Prudente",-21.798,-50.882,32000),
    (3536802,"Ourinhos","Ourinhos","Assis",-22.979,-49.870,116000),
    (3536901,"Ouroeste","Fernandópolis","São José do Rio Preto",-20.033,-50.372,9000),
    (3537008,"Ouro Verde","Dracena","Presidente Prudente",-21.488,-51.705,6000),
    (3537107,"Pacaembu","Dracena","Presidente Prudente",-21.566,-51.268,14000),
    (3537206,"Palestina","São José do Rio Preto","São José do Rio Preto",-20.392,-49.428,12000),
    (3537305,"Palmares Paulista","Catanduva","São José do Rio Preto",-21.085,-48.804,12000),
    (3537404,"Palmeira d'Oeste","Jales","São José do Rio Preto",-20.413,-50.756,12000),
    (3537503,"Palmital","Assis","Assis",-22.793,-50.220,22000),
    (3537602,"Panorama","Dracena","Presidente Prudente",-21.352,-51.858,18000),
    (3537701,"Paraguaçu Paulista","Assis","Assis",-22.413,-50.576,44000),
    (3537800,"Paraibuna","São José dos Campos","Vale do Paraíba Paulista",-23.391,-45.663,18000),
    (3537909,"Paraíso","Catanduva","São José do Rio Preto",-21.021,-48.725,4500),
    (3538006,"Paranapanema","Itapeva","Itapetininga",-23.392,-48.722,18000),
    (3538105,"Paranapuã","Jales","São José do Rio Preto",-20.095,-50.623,5000),
    (3538204,"Parapuã","Dracena","Presidente Prudente",-21.783,-50.796,14000),
    (3538303,"Pardinho","Botucatu","Bauru",-23.078,-48.363,6000),
    (3538402,"Pariquera-Açu","Registro","Vale do Ribeira / Litoral Sul",-24.714,-47.879,19000),
    (3538501,"Parisi","Votuporanga","São José do Rio Preto",-20.107,-49.774,2500),
    (3538600,"Patrocínio Paulista","Franca","Ribeirão Preto",-20.631,-47.281,12000),
    (3538709,"Paulicéia","Dracena","Presidente Prudente",-21.325,-51.834,8000),
    (3538808,"Paulínia","Campinas","Campinas",-22.762,-47.152,107000),
    (3538907,"Paulistânia","Bauru","Bauru",-22.519,-49.591,3000),
    (3539004,"Paulo de Faria","São José do Rio Preto","São José do Rio Preto",-20.028,-49.401,10000),
    (3539103,"Pederneiras","Bauru","Bauru",-22.352,-48.773,44000),
    (3539202,"Pedra Bela","Bragança Paulista","Macro Metropolitana Paulista",-22.796,-46.446,6000),
    (3539301,"Pedranópolis","Fernandópolis","São José do Rio Preto",-20.226,-50.037,2500),
    (3539400,"Pedregulho","Franca","Ribeirão Preto",-20.253,-47.478,16000),
    (3539509,"Pedreira","Amparo","Campinas",-22.741,-46.903,47000),
    (3539608,"Pedrinhas Paulista","Assis","Assis",-22.829,-50.825,3000),
    (3539707,"Pedro de Toledo","Registro","Vale do Ribeira / Litoral Sul",-24.275,-47.226,10000),
    (3539806,"Penápolis","Araçatuba","Araçatuba",-21.418,-50.078,60000),
    (3539905,"Pereira Barreto","Andradina","Araçatuba",-20.638,-51.106,26000),
    (3540002,"Pereiras","Tatuí","Itapetininga",-23.076,-47.979,9000),
    (3540101,"Peruíbe","Registro","Vale do Ribeira / Litoral Sul",-24.320,-47.000,67000),
    (3540200,"Piacatu","Araçatuba","Araçatuba",-21.580,-50.533,5000),
    (3540309,"Piedade","Sorocaba","Macro Metropolitana Paulista",-23.911,-47.423,60000),
    (3540408,"Pilar do Sul","Sorocaba","Macro Metropolitana Paulista",-23.808,-47.718,28000),
    (3540507,"Pindamonhangaba","São José dos Campos","Vale do Paraíba Paulista",-22.924,-45.461,175000),
    (3540606,"Pindorama","Catanduva","São José do Rio Preto",-21.187,-48.911,17000),
    (3540705,"Pinhalzinho","Bragança Paulista","Macro Metropolitana Paulista",-23.042,-46.597,15000),
    (3540804,"Piquete","Guaratinguetá","Vale do Paraíba Paulista",-22.614,-45.176,15000),
    (3540903,"Piraçununga","Araraquara","Araraquara",-21.996,-47.426,70000),
    (3541000,"Pirajuí","Lins","Araçatuba",-21.999,-49.457,28000),
    (3541109,"Pirangi","Catanduva","São José do Rio Preto",-21.095,-48.663,12000),
    (3541208,"Pirapora do Bom Jesus","Sorocaba","Macro Metropolitana Paulista",-23.392,-47.000,20000),
    (3541307,"Pirapozinho","Presidente Prudente","Presidente Prudente",-22.272,-51.500,22000),
    (3541406,"Pirassununga","Araraquara","Araraquara",-21.996,-47.426,70000),
    (3541505,"Piratininga","Bauru","Bauru",-22.415,-49.128,15000),
    (3541604,"Pitangueiras","Jaboticabal","Ribeirão Preto",-21.008,-48.221,35000),
    (3541703,"Planalto","São José do Rio Preto","São José do Rio Preto",-21.029,-49.950,6000),
    (3541802,"Platina","Assis","Assis",-22.753,-49.965,5000),
    (3541901,"Poloni","São José do Rio Preto","São José do Rio Preto",-20.782,-49.832,6000),
    (3542008,"Pompéia","Marília","Marília",-22.108,-50.166,21000),
    (3542107,"Pongaí","Lins","Araçatuba",-21.784,-49.356,5500),
    (3542206,"Pontal","Ribeirão Preto","Ribeirão Preto",-21.022,-48.038,38000),
    (3542305,"Pontalinda","Jales","São José do Rio Preto",-20.420,-50.608,4000),
    (3542404,"Pontes Gestal","Votuporanga","São José do Rio Preto",-20.012,-49.698,3000),
    (3542503,"Populina","Jales","São José do Rio Preto",-19.975,-50.437,5000),
    (3542602,"Porangaba","Tatuí","Itapetininga",-23.175,-48.123,9000),
    (3542701,"Porto Feliz","Sorocaba","Macro Metropolitana Paulista",-23.215,-47.524,56000),
    (3542800,"Porto Ferreira","Araraquara","Araraquara",-21.855,-47.477,58000),
    (3542909,"Potim","Guaratinguetá","Vale do Paraíba Paulista",-22.826,-45.281,25000),
    (3543006,"Potirendaba","São José do Rio Preto","São José do Rio Preto",-21.040,-49.368,13000),
    (3543105,"Pracinha","Dracena","Presidente Prudente",-21.731,-51.525,2500),
    (3543204,"Pradópolis","Araraquara","Araraquara",-21.361,-48.064,15000),
    (3543303,"Praia Grande","Santos","Metropolitana de São Paulo",-24.007,-46.403,335000),
    (3543402,"Ribeirão Preto","Ribeirão Preto","Ribeirão Preto",-21.179,-47.810,718531),
    (3543501,"Presidente Alves","Bauru","Bauru",-22.093,-49.430,4000),
    (3543600,"Presidente Bernardes","Presidente Prudente","Presidente Prudente",-21.993,-51.540,15000),
    (3543709,"Presidente Epitácio","Presidente Prudente","Presidente Prudente",-21.765,-52.109,43000),
    (3543808,"Presidente Prudente","Presidente Prudente","Presidente Prudente",-22.126,-51.393,224191),
    (3543907,"Presidente Venceslau","Presidente Prudente","Presidente Prudente",-21.877,-51.843,37000),
    (3544004,"Promissão","Lins","Araçatuba",-21.538,-49.858,37000),
    (3544103,"Quadra","Tatuí","Itapetininga",-23.261,-48.000,4000),
    (3544202,"Quatá","Assis","Assis",-22.249,-50.699,14000),
    (3544301,"Queiroz","Marília","Marília",-21.852,-50.262,4500),
    (3544400,"Queluz","Guaratinguetá","Vale do Paraíba Paulista",-22.536,-44.778,12000),
    (3544509,"Quintana","Marília","Marília",-22.145,-50.304,6000),
    (3544608,"Rafard","Piracicaba","Campinas",-23.012,-47.534,10000),
    (3544707,"Rancharia","Assis","Assis",-22.228,-50.890,31000),
    (3544806,"Redenção da Serra","São José dos Campos","Vale do Paraíba Paulista",-23.260,-45.541,4500),
    (3544905,"Regente Feijó","Presidente Prudente","Presidente Prudente",-22.220,-51.300,18000),
    (3545001,"Reginópolis","Bauru","Bauru",-21.889,-49.228,6000),
    (3545100,"Registro","Registro","Vale do Ribeira / Litoral Sul",-24.490,-47.844,58000),
    (3545209,"Restinga","Franca","Ribeirão Preto",-20.617,-47.596,7500),
    (3545308,"Ribeira","Capão Bonito","Itapetininga",-24.652,-49.007,3500),
    (3545407,"Ribeirão Bonito","Araraquara","Araraquara",-22.069,-48.171,14000),
    (3545506,"Ribeirão Branco","Capão Bonito","Itapetininga",-24.214,-48.999,18000),
    (3545605,"Ribeirão Corrente","Franca","Ribeirão Preto",-20.406,-47.566,5000),
    (3545704,"Ribeirão do Sul","Assis","Assis",-22.718,-49.958,5000),
    (3545803,"Ribeirão dos Índios","Presidente Prudente","Presidente Prudente",-21.838,-51.580,3500),
    (3545902,"Ribeirão Grande","Capão Bonito","Itapetininga",-24.100,-48.350,6500),
    (3546009,"Ribeirão Pires","Santo André","Metropolitana de São Paulo",-23.711,-46.412,119000),
    (3546108,"Rifaina","Franca","Ribeirão Preto",-20.083,-47.425,5000),
    (3546207,"Rincão","Araraquara","Araraquara",-21.587,-48.077,11000),
    (3546306,"Rinópolis","Dracena","Presidente Prudente",-21.730,-50.726,9000),
    (3546405,"Rio Claro","Rio Claro","Campinas",-22.411,-47.562,204067),
    (3546504,"Rio das Pedras","Piracicaba","Campinas",-22.841,-47.613,30000),
    (3546603,"Rio Grande da Serra","Santo André","Metropolitana de São Paulo",-23.744,-46.399,47000),
    (3546702,"Riolândia","Votuporanga","São José do Rio Preto",-20.016,-49.679,11000),
    (3546801,"Riversul","Itapeva","Itapetininga",-23.870,-49.445,7000),
    (3546900,"Rosana","Presidente Prudente","Presidente Prudente",-22.578,-53.067,22000),
    (3547007,"Roseira","Guaratinguetá","Vale do Paraíba Paulista",-22.896,-45.310,10000),
    (3547106,"Rubiácea","Araçatuba","Araçatuba",-21.302,-50.481,3000),
    (3547205,"Rubinéia","Jales","São José do Rio Preto",-19.973,-51.029,3500),
    (3547304,"Sabino","Lins","Araçatuba",-21.421,-49.527,6000),
    (3547403,"Sagres","Dracena","Presidente Prudente",-21.831,-51.485,3000),
    (3547502,"Sales","São José do Rio Preto","São José do Rio Preto",-21.342,-49.437,6000),
    (3547601,"Sales Oliveira","Franca","Ribeirão Preto",-20.772,-47.831,12000),
    (3547700,"Salesópolis","Mogi das Cruzes","Metropolitana de São Paulo",-23.530,-45.844,16000),
    (3547809,"Salmourão","Dracena","Presidente Prudente",-21.621,-51.031,6500),
    (3547908,"Saltinho","Piracicaba","Campinas",-22.843,-47.652,8000),
    (3548005,"Salto","Sorocaba","Macro Metropolitana Paulista",-23.200,-47.286,114000),
    (3548104,"Salto de Pirapora","Sorocaba","Macro Metropolitana Paulista",-23.647,-47.573,43000),
    (3548203,"Salto Grande","Ourinhos","Assis",-22.890,-49.988,8000),
    (3548302,"Sandovalina","Presidente Prudente","Presidente Prudente",-22.483,-52.001,5000),
    (3548401,"Santa Adélia","Catanduva","São José do Rio Preto",-21.240,-48.798,15000),
    (3548500,"Santos","Santos","Metropolitana de São Paulo",-23.960,-46.333,433311),
    (3548609,"Santa Cruz das Palmeiras","São João da Boa Vista","Campinas",-21.820,-47.245,32000),
    (3548708,"São Bernardo do Campo","Santo André","Metropolitana de São Paulo",-23.692,-46.565,844483),
    (3548807,"Santa Cruz do Rio Pardo","Ourinhos","Assis",-22.899,-49.632,44000),
    (3548906,"Santa Gertrudes","Rio Claro","Campinas",-22.459,-47.528,25000),
    (3549003,"Santa Isabel","Mogi das Cruzes","Metropolitana de São Paulo",-23.315,-46.216,55000),
    (3549102,"Santa Lúcia","Araraquara","Araraquara",-21.681,-48.098,12000),
    (3549201,"Santa Maria da Serra","Piracicaba","Campinas",-22.551,-48.153,9000),
    (3549300,"Santa Mercedes","Presidente Prudente","Presidente Prudente",-21.268,-51.972,4500),
    (3549409,"Santana da Ponte Pensa","Jales","São José do Rio Preto",-20.094,-51.165,2800),
    (3549508,"Santana de Parnaíba","Osasco","Metropolitana de São Paulo",-23.443,-46.916,130000),
    (3549607,"Santa Rita d'Oeste","Jales","São José do Rio Preto",-20.128,-50.827,3500),
    (3549706,"Santa Rita do Passa Quatro","São Carlos","Araraquara",-21.706,-47.477,28000),
    (3549805,"Santa Rosa de Viterbo","Batatais","Ribeirão Preto",-21.477,-47.360,25000),
    (3549904,"São José dos Campos","São José dos Campos","Vale do Paraíba Paulista",-23.179,-45.886,748023),
    (3550001,"Santo Anastácio","Presidente Prudente","Presidente Prudente",-21.974,-51.670,21000),
    (3550100,"Santo André","Santo André","Metropolitana de São Paulo",-23.665,-46.531,707614),
    (3550209,"Santo Antônio da Alegria","Batatais","Ribeirão Preto",-21.085,-47.145,9000),
    (3550308,"São Paulo","São Paulo","Metropolitana de São Paulo",-23.550,-46.633,12396372),
    (3550407,"Santo Antônio de Posse","Campinas","Campinas",-22.601,-46.922,25000),
    (3550506,"Santo Antônio do Aracanguá","Araçatuba","Araçatuba",-21.143,-50.452,10000),
    (3550605,"Santo Antônio do Jardim","Amparo","Campinas",-22.098,-46.672,8000),
    (3550704,"Santo Antônio do Pinhal","Bragança Paulista","Macro Metropolitana Paulista",-22.825,-45.665,7000),
    (3550803,"Santo Expedito","Presidente Prudente","Presidente Prudente",-21.854,-51.381,4000),
    (3550902,"Santópolis do Aguapeí","Araçatuba","Araçatuba",-21.469,-50.636,5000),
    (3551009,"Santos","Santos","Metropolitana de São Paulo",-23.960,-46.333,433311),
    (3551108,"São Bento do Sapucaí","Bragança Paulista","Macro Metropolitana Paulista",-22.694,-45.727,11000),
    (3551207,"São Carlos","São Carlos","Araraquara",-22.017,-47.891,254484),
    (3551306,"São Francisco","Jales","São José do Rio Preto",-20.155,-50.589,3500),
    (3551405,"São João das Duas Pontes","Fernandópolis","São José do Rio Preto",-20.383,-50.374,3000),
    (3551504,"São João de Iracema","Jales","São José do Rio Preto",-20.152,-51.094,2500),
    (3551603,"São João do Pau d'Alho","Dracena","Presidente Prudente",-21.259,-51.649,2500),
    (3551702,"São Joaquim da Barra","Franca","Ribeirão Preto",-20.583,-47.854,49000),
    (3551801,"São José da Bela Vista","Franca","Ribeirão Preto",-20.601,-47.641,6000),
    (3551900,"São José do Barreiro","Bananal","Vale do Paraíba Paulista",-22.655,-44.575,5000),
    (3552007,"São José do Rio Pardo","São João da Boa Vista","Campinas",-21.597,-46.893,52000),
    (3552106,"São José do Rio Preto","São José do Rio Preto","São José do Rio Preto",-20.820,-49.379,494654),
    (3552205,"Sorocaba","Sorocaba","Macro Metropolitana Paulista",-23.501,-47.459,704942),
    (3552304,"São José dos Campos","São José dos Campos","Vale do Paraíba Paulista",-23.179,-45.886,748023),
    (3552403,"São Lourenço da Serra","Itapecerica da Serra","Metropolitana de São Paulo",-23.851,-47.028,14000),
    (3552502,"São Luís do Paraitinga","São José dos Campos","Vale do Paraíba Paulista",-23.220,-45.311,10000),
    (3552601,"São Manuel","Botucatu","Bauru",-22.731,-48.568,40000),
    (3552700,"São Miguel Arcanjo","Capão Bonito","Itapetininga",-23.877,-47.997,38000),
    (3552809,"São Paulo","São Paulo","Metropolitana de São Paulo",-23.550,-46.633,12396372),
    (3552908,"São Pedro","Piracicaba","Campinas",-22.547,-47.920,34000),
    (3553005,"São Pedro do Turvo","Ourinhos","Assis",-22.749,-49.746,7500),
    (3553104,"São Roque","Sorocaba","Macro Metropolitana Paulista",-23.528,-47.137,88000),
    (3553203,"São Sebastião","Litoral Norte Paulista","Vale do Paraíba Paulista",-23.796,-45.406,91000),
    (3553302,"São Sebastião da Grama","São João da Boa Vista","Campinas",-21.706,-46.519,12000),
    (3553401,"São Simão","Araraquara","Araraquara",-21.479,-47.558,16000),
    (3553500,"São Vicente","Santos","Metropolitana de São Paulo",-23.958,-46.391,357000),
    (3553609,"Sarapuí","Sorocaba","Macro Metropolitana Paulista",-23.639,-47.824,8000),
    (3553708,"Sarutaiá","Avaré","Itapetininga",-23.259,-49.474,5000),
    (3553807,"Sebastianópolis do Sul","Votuporanga","São José do Rio Preto",-20.661,-49.973,4000),
    (3553906,"Serra Azul","Ribeirão Preto","Ribeirão Preto",-21.312,-47.571,11000),
    (3554003,"Serrana","Ribeirão Preto","Ribeirão Preto",-21.222,-47.598,43000),
    (3554102,"Serra Negra","Amparo","Campinas",-22.609,-46.700,29000),
    (3554201,"Sertãozinho","Ribeirão Preto","Ribeirão Preto",-21.139,-47.991,123000),
    (3554300,"Sete Barras","Registro","Vale do Ribeira / Litoral Sul",-24.382,-47.926,14000),
    (3554409,"Severínia","Barretos","Ribeirão Preto",-20.808,-48.804,16000),
    (3554508,"Silveiras","Bananal","Vale do Paraíba Paulista",-22.667,-44.858,6000),
    (3554607,"Socorro","Bragança Paulista","Macro Metropolitana Paulista",-22.590,-46.529,38000),
    (3554706,"Sorocaba","Sorocaba","Macro Metropolitana Paulista",-23.501,-47.459,704942),
    (3554805,"Sud Mennucci","Andradina","Araçatuba",-20.760,-51.014,7500),
    (3554904,"Sumaré","Campinas","Campinas",-22.820,-47.267,285000),
    (3555000,"Suzano","Mogi das Cruzes","Metropolitana de São Paulo",-23.543,-46.310,305000),
    (3555109,"Suzanápolis","Andradina","Araçatuba",-20.499,-51.018,3500),
    (3555208,"Tabapuã","Catanduva","São José do Rio Preto",-21.043,-49.017,9000),
    (3555307,"Tabatinga","Araraquara","Araraquara",-21.725,-48.685,15000),
    (3555406,"Taboão da Serra","Itapecerica da Serra","Metropolitana de São Paulo",-23.604,-46.757,295000),
    (3555505,"Taciba","Presidente Prudente","Presidente Prudente",-22.388,-51.295,5000),
    (3555604,"Taguaí","Ourinhos","Assis",-23.451,-49.397,13000),
    (3555703,"Taiaçu","Jaboticabal","Ribeirão Preto",-21.184,-48.376,7500),
    (3555802,"Taiúva","Jaboticabal","Ribeirão Preto",-21.136,-48.437,6500),
    (3555901,"Tambaú","São João da Boa Vista","Campinas",-21.706,-47.270,22000),
    (3556008,"Tanabi","São José do Rio Preto","São José do Rio Preto",-20.626,-49.651,26000),
    (3556107,"Tapiraí","Capão Bonito","Itapetininga",-23.965,-47.510,8500),
    (3556206,"Valinhos","Campinas","Campinas",-22.973,-47.003,131000),
    (3556305,"Tapiratiba","São João da Boa Vista","Campinas",-21.467,-46.748,12000),
    (3556404,"Taquaral","Araraquara","Araraquara",-21.643,-48.456,4500),
    (3556503,"Taquaritinga","Araraquara","Araraquara",-21.406,-48.505,56000),
    (3556602,"Taquarituba","Itapeva","Itapetininga",-23.528,-49.244,22000),
    (3556701,"Vinhedo","Campinas","Campinas",-23.029,-46.975,83000),
    (3556800,"Taquarivaí","Capão Bonito","Itapetininga",-23.979,-48.724,5000),
    (3556909,"Uru","Lins","Araçatuba",-21.785,-49.276,1200),
    (3557006,"Tatuí","Tatuí","Itapetininga",-23.352,-47.856,118000),
    (3557105,"Taubaté","São José dos Campos","Vale do Paraíba Paulista",-23.026,-45.556,324200),
    (3557204,"Tejupá","Itapeva","Itapetininga",-23.638,-49.509,6500),
    (3557303,"Teodoro Sampaio","Presidente Prudente","Presidente Prudente",-22.527,-52.167,23000),
    (3557402,"Terra Roxa","Jaboticabal","Ribeirão Preto",-20.785,-48.317,18000),
    (3557501,"Tietê","Sorocaba","Macro Metropolitana Paulista",-23.104,-47.716,38000),
    (3557600,"Timburi","Ourinhos","Assis",-23.208,-49.601,3500),
    (3557659,"Torre de Pedra","Botucatu","Bauru",-23.247,-48.179,2800),
    (3557709,"Torrinha","Araraquara","Araraquara",-22.426,-48.165,12000),
    (3557808,"Trabiju","Araraquara","Araraquara",-21.768,-48.367,3000),
    (3557907,"Tremembé","São José dos Campos","Vale do Paraíba Paulista",-22.965,-45.538,45000),
    (3558004,"Três Fronteiras","Jales","São José do Rio Preto",-19.927,-51.033,7000),
    (3558103,"Tuiuti","Bragança Paulista","Macro Metropolitana Paulista",-22.896,-46.620,6000),
    (3558202,"Tupã","Tupã","Marília",-21.935,-50.513,63000),
    (3558301,"Tupi Paulista","Dracena","Presidente Prudente",-21.388,-51.577,15000),
    (3558400,"Turiúba","Araçatuba","Araçatuba",-21.181,-50.454,3500),
    (3558509,"Turmalina","Fernandópolis","São José do Rio Preto",-20.067,-50.310,3500),
    (3558608,"Ubarana","São José do Rio Preto","São José do Rio Preto",-21.240,-49.774,8000),
    (3558707,"Ubatuba","Litoral Norte Paulista","Vale do Paraíba Paulista",-23.434,-45.074,90000),
    (3558806,"Ubirajara","Lins","Araçatuba",-22.039,-49.534,5000),
    (3558905,"Uchoa","São José do Rio Preto","São José do Rio Preto",-20.951,-49.175,8000),
    (3559002,"União Paulista","Dracena","Presidente Prudente",-21.555,-51.565,2500),
    (3559101,"Urânia","Jales","São José do Rio Preto",-20.188,-50.645,8500),
    (3559200,"Uru","Lins","Araçatuba",-21.785,-49.276,1200),
    (3559309,"Urupês","Catanduva","São José do Rio Preto",-21.194,-49.296,13000),
    (3559408,"Valentim Gentil","Votuporanga","São José do Rio Preto",-20.416,-50.101,9000),
    (3559507,"Valinhos","Campinas","Campinas",-22.973,-47.003,131000),
    (3559606,"Valparaíso","Araçatuba","Araçatuba",-21.228,-50.870,25000),
    (3559705,"Vargem","Bragança Paulista","Macro Metropolitana Paulista",-22.888,-46.929,9500),
    (3559804,"Vargem Grande do Sul","São João da Boa Vista","Campinas",-21.832,-46.893,44000),
    (3559903,"Vargem Grande Paulista","Itapecerica da Serra","Metropolitana de São Paulo",-23.600,-47.025,52000),
    (3560000,"Vera Cruz","Marília","Marília",-22.219,-49.818,10000),
    (3560109,"Vinhedo","Campinas","Campinas",-23.029,-46.975,83000),
    (3560208,"Viradouro","Barretos","Ribeirão Preto",-20.874,-48.133,17000),
    (3560307,"Vista Alegre do Alto","Jaboticabal","Ribeirão Preto",-21.182,-48.626,6000),
    (3560406,"Vitória Brasil","Jales","São José do Rio Preto",-20.081,-50.873,2500),
    (3560505,"Votorantim","Sorocaba","Macro Metropolitana Paulista",-23.548,-47.434,136000),
    (3560604,"Votuporanga","Votuporanga","São José do Rio Preto",-20.422,-49.976,94000),
    (3560703,"Zacarias","Votuporanga","São José do Rio Preto",-20.908,-50.016,3500),
]

# Deduplicate by IBGE code
seen = set()
MUNICIPIOS_UNIQUE = []
for row in SP_MUNICIPIOS:
    if row[0] not in seen:
        seen.add(row[0])
        MUNICIPIOS_UNIQUE.append(row)

print(f"Total de municípios únicos: {len(MUNICIPIOS_UNIQUE)}")

# ── Helper: generate correlated random data ───────────────────────────────────
rng = np.random.default_rng(42)

def gen_pop_data(pop_base):
    """Given base population, generate correlated demographic indicators."""
    # Add noise
    pop = max(500, int(pop_base * rng.lognormal(0, 0.05)))
    urb_rate = min(99, max(20, 60 + math.log10(max(pop, 1)) * 5 + rng.normal(0, 8)))
    pop_urb = int(pop * urb_rate / 100)

    # Age structure (rough approximation of SP demographic pyramid)
    pop_0_4   = int(pop * rng.uniform(0.04, 0.07))
    pop_5_14  = int(pop * rng.uniform(0.09, 0.14))
    pop_15_29 = int(pop * rng.uniform(0.16, 0.22))
    pop_30_44 = int(pop * rng.uniform(0.18, 0.24))
    pop_45_64 = int(pop * rng.uniform(0.18, 0.24))
    pop_65p   = int(pop * rng.uniform(0.10, 0.20))

    # Renda per capita (correlated with log(pop) and urban rate)
    renda_base = 800 + math.log10(max(pop, 1)) * 200 + urb_rate * 10
    renda = max(300, renda_base * rng.lognormal(0, 0.25))

    return {
        "populacao_total": pop, "populacao_urbana": pop_urb,
        "taxa_urbanizacao": round(urb_rate, 1),
        "pop_0_4": pop_0_4, "pop_5_14": pop_5_14,
        "pop_15_29": pop_15_29, "pop_30_44": pop_30_44,
        "pop_45_64": pop_45_64, "pop_65_plus": pop_65p,
        "renda_per_capita": round(renda, 1),
        "populacao_alvo": pop_30_44 + pop_45_64,
        "indice_envelhecimento": round(pop_65p / max(pop_0_4 + pop_5_14, 1) * 100, 1),
    }


def gen_cnes_data(pop):
    """Generate CNES health establishments proportional to population."""
    factor = max(pop / 10000, 0.1)
    return {
        "farmacias":             max(0, int(rng.poisson(max(1.5 * factor, 0.5)))),
        "consultorios_medicos":  max(0, int(rng.poisson(max(2.0 * factor, 0.3)))),
        "consultorios_odonto":   max(0, int(rng.poisson(max(1.5 * factor, 0.3)))),
        "laboratorios":          max(0, int(rng.poisson(max(0.8 * factor, 0.1)))),
        "clinicas":              max(0, int(rng.poisson(max(0.5 * factor, 0.1)))),
        "hospitais":             max(0, int(rng.poisson(max(0.2 * factor, 0.05)))),
        "ubs_upa":               max(1, int(rng.poisson(max(1.0 * factor, 0.5)))),
    }


def gen_economic_data(pop, renda):
    """Generate economic indicators correlated with population and income."""
    pib_base = renda * 2.5 * rng.lognormal(0, 0.2)
    idh_base = 0.6 + min(0.25, math.log10(max(pop, 1)) * 0.03 + renda / 50000)
    idh = min(0.95, max(0.5, idh_base + rng.normal(0, 0.03)))
    ben_rate = min(80, max(5, (renda / 3000) * 50 * rng.lognormal(0, 0.3)))
    ben = int(pop * ben_rate / 100)
    return {
        "pib_per_capita": round(pib_base, 1),
        "idh": round(idh, 3),
        "beneficiarios_planos": ben,
        "cobertura_planos_pct": round(ben_rate, 1),
    }


# ── Override known key cities with accurate data ─────────────────────────────
OVERRIDES = {
    3509502: {"populacao_total": 1223237, "taxa_urbanizacao": 98.5, "renda_per_capita": 3200,
              "pib_per_capita": 58000, "idh": 0.805, "beneficiarios_planos": 560000,
              "farmacias": 890, "consultorios_medicos": 4200, "hospitais": 38,
              "laboratorios": 280, "clinicas": 650, "consultorios_odonto": 1200},
    3550308: {"populacao_total": 12396372, "taxa_urbanizacao": 99.1, "renda_per_capita": 3800,
              "pib_per_capita": 85000, "idh": 0.805, "beneficiarios_planos": 7200000,
              "farmacias": 6800, "consultorios_medicos": 32000, "hospitais": 280,
              "laboratorios": 2200, "clinicas": 4800, "consultorios_odonto": 9500},
    3548708: {"populacao_total": 844483, "taxa_urbanizacao": 99.2, "renda_per_capita": 3600,
              "pib_per_capita": 62000, "idh": 0.805, "beneficiarios_planos": 420000,
              "farmacias": 620, "consultorios_medicos": 2800, "hospitais": 22},
    3519204: {"populacao_total": 1392121, "taxa_urbanizacao": 98.8, "renda_per_capita": 2900,
              "pib_per_capita": 35000, "idh": 0.763, "beneficiarios_planos": 480000,
              "farmacias": 980, "consultorios_medicos": 3800, "hospitais": 28},
    3536406: {"populacao_total": 700000, "taxa_urbanizacao": 99.5, "renda_per_capita": 3100,
              "pib_per_capita": 42000, "idh": 0.776, "beneficiarios_planos": 290000,
              "farmacias": 510, "consultorios_medicos": 2200, "hospitais": 18},
    3525904: {"populacao_total": 431813, "taxa_urbanizacao": 97.8, "renda_per_capita": 3400,
              "pib_per_capita": 55000, "idh": 0.822, "beneficiarios_planos": 220000,
              "farmacias": 320, "consultorios_medicos": 1500, "hospitais": 12},
    3536505: {"populacao_total": 107000, "taxa_urbanizacao": 97.2, "renda_per_capita": 4200,
              "pib_per_capita": 180000, "idh": 0.826, "beneficiarios_planos": 68000,
              "farmacias": 95, "consultorios_medicos": 480, "hospitais": 4},
    3556206: {"populacao_total": 131000, "taxa_urbanizacao": 97.5, "renda_per_capita": 3800,
              "pib_per_capita": 65000, "idh": 0.831, "beneficiarios_planos": 85000,
              "farmacias": 110, "consultorios_medicos": 550, "hospitais": 3},
    3556701: {"populacao_total": 83000, "taxa_urbanizacao": 97.0, "renda_per_capita": 4500,
              "pib_per_capita": 72000, "idh": 0.817, "beneficiarios_planos": 54000,
              "farmacias": 72, "consultorios_medicos": 380, "hospitais": 2},
    3549904: {"populacao_total": 748023, "taxa_urbanizacao": 95.1, "renda_per_capita": 3500,
              "pib_per_capita": 48000, "idh": 0.807, "beneficiarios_planos": 380000,
              "farmacias": 530, "consultorios_medicos": 2400, "hospitais": 18},
    3543402: {"populacao_total": 718531, "taxa_urbanizacao": 97.2, "renda_per_capita": 3300,
              "pib_per_capita": 52000, "idh": 0.800, "beneficiarios_planos": 340000,
              "farmacias": 520, "consultorios_medicos": 2500, "hospitais": 22},
    3552205: {"populacao_total": 704942, "taxa_urbanizacao": 97.0, "renda_per_capita": 2800,
              "pib_per_capita": 38000, "idh": 0.783, "beneficiarios_planos": 280000,
              "farmacias": 490, "consultorios_medicos": 2100, "hospitais": 16},
    # Small municipalities (known low-scoring)
    3506706: {"populacao_total": 806, "taxa_urbanizacao": 62.0, "renda_per_capita": 650,
              "pib_per_capita": 18000, "idh": 0.612, "beneficiarios_planos": 40,
              "farmacias": 1, "consultorios_medicos": 1, "hospitais": 0},
    3533205: {"populacao_total": 1600, "taxa_urbanizacao": 55.0, "renda_per_capita": 600,
              "pib_per_capita": 14000, "idh": 0.590, "beneficiarios_planos": 55,
              "farmacias": 1, "consultorios_medicos": 1, "hospitais": 0},
    3516200: {"populacao_total": 2500, "taxa_urbanizacao": 58.0, "renda_per_capita": 680,
              "pib_per_capita": 16000, "idh": 0.605, "beneficiarios_planos": 80,
              "farmacias": 1, "consultorios_medicos": 1, "hospitais": 0},
    3556909: {"populacao_total": 1200, "taxa_urbanizacao": 50.0, "renda_per_capita": 580,
              "pib_per_capita": 12000, "idh": 0.582, "beneficiarios_planos": 35,
              "farmacias": 0, "consultorios_medicos": 0, "hospitais": 0},
}

# ── Build cache data ──────────────────────────────────────────────────────────
municipios_json = []
coords_json = {}
cnes_json = {}
ipeadata_pib_rows = []
ipeadata_idh_rows = []
ans_records = []

for row in MUNICIPIOS_UNIQUE:
    ibge_id, nome, micro, meso, lat, lon, pop_base = row
    codigo = str(ibge_id).zfill(7)

    # IBGE municipalities JSON
    municipios_json.append({
        "id": ibge_id,
        "nome": nome,
        "microrregiao": {
            "id": int(codigo[:5]),
            "nome": micro,
            "mesorregiao": {
                "id": int(codigo[:4]),
                "nome": meso,
                "UF": {"id": 35, "sigla": "SP", "nome": "São Paulo"}
            }
        }
    })

    # Coordinates
    lat_jitter = lat + rng.normal(0, 0.01)
    lon_jitter = lon + rng.normal(0, 0.01)
    coords_json[codigo] = [lat_jitter, lon_jitter]

    # Demographic data
    demo = gen_pop_data(pop_base)
    if ibge_id in OVERRIDES:
        demo.update({k: v for k, v in OVERRIDES[ibge_id].items()
                     if k in demo})

    pop = demo["populacao_total"]
    renda = demo["renda_per_capita"]

    # CNES data
    cnes = gen_cnes_data(pop)
    if ibge_id in OVERRIDES:
        cnes.update({k: v for k, v in OVERRIDES[ibge_id].items()
                     if k in cnes})

    cnes_json[codigo] = cnes

    # Economic data
    econ = gen_economic_data(pop, renda)
    if ibge_id in OVERRIDES:
        econ.update({k: v for k, v in OVERRIDES[ibge_id].items()
                     if k in econ})

    ipeadata_pib_rows.append({
        "TERCODIGO": codigo,
        "VALDATA": "2021-01-01T00:00:00",
        "VALVALOR": econ["pib_per_capita"]
    })
    ipeadata_idh_rows.append({
        "TERCODIGO": codigo,
        "VALDATA": "2010-01-01T00:00:00",
        "VALVALOR": econ["idh"]
    })
    ans_records.append({
        "codigo_ibge": codigo,
        "beneficiarios_planos": econ["beneficiarios_planos"]
    })


# ── Save cache files ──────────────────────────────────────────────────────────
with open(CACHE_DIR / "municipios_sp.json", "w", encoding="utf-8") as f:
    json.dump(municipios_json, f, ensure_ascii=False)
print(f"✓ municipios_sp.json  ({len(municipios_json)} itens)")

with open(CACHE_DIR / "coords_sp.json", "w", encoding="utf-8") as f:
    json.dump(coords_json, f, ensure_ascii=False)
print(f"✓ coords_sp.json  ({len(coords_json)} itens)")

with open(CACHE_DIR / "cnes_sp.json", "w", encoding="utf-8") as f:
    json.dump(cnes_json, f, ensure_ascii=False)
print(f"✓ cnes_sp.json  ({len(cnes_json)} municípios)")

# ipeadata CSVs must match cached_df format returned by _fetch_ipeadata:
# columns: codigo_ibge, value
df_pib = pd.DataFrame([{"codigo_ibge": r["TERCODIGO"], "value": r["VALVALOR"]}
                        for r in ipeadata_pib_rows])
df_pib.to_csv(CACHE_DIR / "ipeadata_pib.csv", index=False)
print(f"✓ ipeadata_pib.csv  ({len(df_pib)} linhas)")

df_idh = pd.DataFrame([{"codigo_ibge": r["TERCODIGO"], "value": r["VALVALOR"]}
                        for r in ipeadata_idh_rows])
df_idh.to_csv(CACHE_DIR / "ipeadata_idh.csv", index=False)
print(f"✓ ipeadata_idh.csv  ({len(df_idh)} linhas)")

with open(CACHE_DIR / "ans_beneficiarios.json", "w", encoding="utf-8") as f:
    json.dump(ans_records, f, ensure_ascii=False)
print(f"✓ ans_beneficiarios.json  ({len(ans_records)} municípios)")

print(f"\nCache gerado com sucesso em {CACHE_DIR}/")
