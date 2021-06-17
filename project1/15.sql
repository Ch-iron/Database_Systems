select t.id, count(*) count from Trainer t, CatchedPokemon cp where t.id = cp.owner_id group by t.name 
having count(*) = (select max(m.count) from (select count(*) count from Trainer t, CatchedPokemon cp where t.id = cp.owner_id group by t.name) m) 
order by t.id;