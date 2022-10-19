CREATE TABLE IF NOT EXISTS `t_order_20221010`
(
    `id`          BIGINT(20)  NOT NULL,
    `create_time` datetime(3) not null,
    `comment`     VARCHAR(30) NULL DEFAULT NULL,
    PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `t_order_20221011`
(
    `id`          BIGINT(20)  NOT NULL,
    `create_time` datetime(3) not null,
    `comment`     VARCHAR(30) NOT NULL DEFAULT NULL,
    PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `t_order_20221012`
(
    `id`          BIGINT(20)  NOT NULL,
    `create_time` datetime(3) not null,
    `comment`     VARCHAR(30) NOT NULL DEFAULT NULL,
    PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `t_order_20221013`
(
    `id`          BIGINT(20)  NOT NULL,
    `create_time` datetime(3) not null,
    `comment`     VARCHAR(30) NOT NULL DEFAULT NULL,
    PRIMARY KEY (`id`)
);

insert into `t_order_20221010`(`id`,`create_time`,`comment`) values (1,'2022-10-10 00:00:00.000','test');
insert into `t_order_20221011`(`id`,`create_time`,`comment`) values (1,'2022-10-11 00:00:00.000','test');
insert into `t_order_20221012`(`id`,`create_time`,`comment`) values (1,'2022-10-12 00:00:00.000','test');
insert into `t_order_20221013`(`id`,`create_time`,`comment`) values (1,'2022-10-13 00:00:00.000','test');
