select e.id, e.First_Evolution, e.Second_Evolution, p.name "Third_Evolution" from Pokemon p, 
(select e.id, e.First_Evolution, p.name "Second_Evolution", e.Third from Pokemon p, 
(select p.id, p.name "First_Evolution", e.Second, e.Third from Pokemon p, 
(select a.before_id "First", a.after_id "Second", b.after_id "Third" from Evolution a, Evolution b 
where a.after_id = b.before_id) e where p.id = e.First) e where p.id = e.Second) e where p.id = e.Third order by e.id;