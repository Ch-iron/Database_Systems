select p.name, cp.pid from Trainer t, Pokemon p, CatchedPokemon cp where t.id = cp.owner_id and p.id = cp.pid and t.hometown = "Sangnok City" order by cp.pid;