CREATE TABLE `person` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `first_name` varchar(120) NOT NULL,
  `last_name` varchar(120) NOT NULL,
  `birth_date` date DEFAULT NULL,
  `deleted` varchar(5) NOT NULL DEFAULT 'false',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8;


INSERT INTO `person` (`id`, `first_name`, `last_name`, `birth_date`, `deleted`)
VALUES
	(1, 'John', 'travaolta', '1980-01-01', 'false'),
	(2, 'boris', 'becker', '1981-02-02', 'false'),
	(3, 'sunil', 'gavaskar', '1982-03-03', 'false'),
	(4, 'shoury', 'bhardwaj', '1983-04-04', 'false'),
	(5, 'jagadeesh', 'huliyar', '1984-05-05', 'false'),
	(6, 'sajid', 'nadiawala', '1985-06-06', 'false'),
	(7, 'regunath', 'b', '1984-05-05', 'false'),
	(8, 'greg', 'chappel', '1985-06-06', 'false'),
	(9, 'kapil', 'dev', '1986-07-07', 'false'),
	(10, 'chandan', 'bansal', '1986-07-07', 'false');
