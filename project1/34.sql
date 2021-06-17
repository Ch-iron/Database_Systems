select p.name, cp.level, cp.nickname from Gym g, Pokemon p, CatchedPokemon cp where g.leader_id = cp.owner_id and p.id = cp.pid 
and cp.nickname like "A%" order by p.name desc;