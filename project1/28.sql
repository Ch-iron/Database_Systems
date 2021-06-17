select t.name, avg(cp.level) "Average Trainer's Pokemon level(Normal & Electric)"  
from Trainer t, (select * from Pokemon where type = "Normal" or type = "Electric") p, Catchedpokemon cp 
where t.id = cp.owner_id and p.id = cp.pid group by t.name order by avg(cp.level);