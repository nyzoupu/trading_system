-- trading_system.users definition
drop table users;
CREATE TABLE `users` (
   id INT AUTO_INCREMENT PRIMARY key,
  `email` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL,
  `password` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL,
  `age` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;