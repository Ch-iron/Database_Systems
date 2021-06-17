select distinct s.name from 
(select p.name, cp.pid from Trainer t, Pokemon p, CatchedPokemon cp where t.id = cp.owner_id and p.id = cp.pid and t.hometown = "Sangnok City") s, 
(select p.name, cp.pid from Trainer t, Pokemon p, CatchedPokemon cp where t.id = cp.owner_id and p.id = cp.pid and t.hometown = "Brown City") b
where s.pid = b.pid order by s.name;