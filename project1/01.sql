select t.name from Trainer t, CatchedPokemon cp where t.id = cp.owner_id group by t.name having count(cp.pid) >= 3 order by count(cp.pid) desc;