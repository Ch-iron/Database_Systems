select t.name, avg(cp.level) "Average level" from Trainer t, Gym g, CatchedPokemon cp where t.id = g.leader_id and g.leader_id = cp.owner_id 
group by t.name order by t.name;