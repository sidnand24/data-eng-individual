create schema nba;


drop table if exists nba.teams cascade;
create table nba.teams (
    team_id         int primary key,
    full_name       varchar(128),
    abbreviation    varchar(128),
    nickname        varchar(128),
    city            varchar(128),
    state           varchar(128),
    year_founded    int
);



drop table if exists nba.players cascade;
create table nba.players (
    player_id       int primary key,
    first_name      varchar(128),
    last_name       varchar(128),
    team_id         int references nba.teams("team_id") on delete cascade on update cascade,
    position        varchar(128),
    age             int,
    height          int,
    weight          int,
    college         varchar(128),
    salary          int
);



drop table if exists nba.stats cascade;
create table nba.stats (
    player_id       int references nba.players("player_id") on delete cascade on update cascade,
    G               int,
    GS              int,
    MP              int,
    FG              int,
    FGA             int,
    FG_PCT          real,
    3P              int,
    3PA             int,
    3P_PCT          real,
    2P              int,
    2PA             int,
    2P_PCT          real,
    eFG_PCT         real,
    FT              int,
    FTA             int,
    FT_PCT          real,
    ORB             int,
    DRB             int,
    TRB             int,
    AST             int,
    STL             int,
    BLK             int,
    TOV             int,
    PF              int,
    PTS             int
);


drop table if exists nba.tweets cascade;
create table nba.tweets (
    tweet_id        int primary key,
    player_id       int references nba.players("player_id") on delete cascade on update cascade,
    tweet_date      varchar(256),
    tweet_count     int
);

