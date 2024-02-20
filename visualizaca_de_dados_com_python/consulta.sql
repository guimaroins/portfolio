select 
    controle, 		
    ano, 
	mes, 
	ano_fato, 
	mes_fato, 
	titulo, 
	titulo_do, 
	conteudo, 
	data_com, 
	data_fato, 
	idade, 
	municipio_fato, 
	sexo, 
	cor, 
	relacao,
	case 
		when idade < '18' then 1		
		else 0
	end	as "Crianças e Adolescentes",
	case
		when idade between '18' and '30' then 1
		else 0
	end	as Jovens,
	case
		when idade >= '65' then 1
		else 0
	end	as Idosos,
	case
		when cor in ('negra', 'parda') then 1
		else 0
	end	as "População Negra",
	case
		when sexo = 'feminino' then 1
		else 0
	end	as Mulheres
from grupos_vulneraveis.todos
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 
order by data_com, controle