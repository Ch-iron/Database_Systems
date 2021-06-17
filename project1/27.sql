select t.name, max(cp.level) "Max level of Trainer's Pokemon"  from Trainer t, CatchedPokemon cp where t.id = cp.owner_id 
group by t.name having count(*) >= 4 order by t.name;