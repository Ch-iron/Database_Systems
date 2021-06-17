select cp.nickname from Pokemon p, CatchedPokemon cp where p.id = cp.pid and cp.level >= 50 and cp.owner_id >= 6 order by cp.nickname;