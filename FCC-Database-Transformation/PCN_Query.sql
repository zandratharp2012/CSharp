SELECT a.* FROM PCN.dbo.MasterFile as a
WHERE a.freq1_1 LIKE '6123%' 
AND a.state1 = 'CA'
AND site1 LIKE 'HUMBU%' OR site2 LIKE 'HUMBU%'