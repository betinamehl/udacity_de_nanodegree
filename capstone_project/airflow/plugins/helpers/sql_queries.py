class SqlQueries:
    
    
    create_staging_population = """ create table if not exists raw_data.brazil_population (
                                                    CITY varchar,
                                                    STATE varchar,
                                                    CAPITAL boolean,
                                                    IBGE_RES_POP integer,
                                                    IBGE_RES_POP_BRAS integer,
                                                    IBGE_RES_POP_ESTR integer,
                                                    IBGE_DU integer,
                                                    IBGE_DU_URBAN integer,
                                                    IBGE_DU_RURAL integer
                                                )
                                            """
    
    
    create_staging_candidates = """
                                    create table if not exists raw_data.election_candidates (
                                        DT_GERACAO varchar,
                                        HH_GERACAO varchar,
                                        ANO_ELEICAO	varchar,
                                        CD_TIPO_ELEICAO	varchar,
                                        NM_TIPO_ELEICAO	varchar,
                                        NR_TURNO	varchar,
                                        CD_ELEICAO	varchar,
                                        DS_ELEICAO	varchar,
                                        DT_ELEICAO	varchar,
                                        TP_ABRANGENCIA	varchar,
                                        SG_UF	varchar,
                                        SG_UE	varchar,
                                        NM_UE	varchar,
                                        CD_CARGO	varchar,
                                        DS_CARGO	varchar,
                                        SQ_CANDIDATO	varchar,
                                        NR_CANDIDATO	varchar,
                                        NM_CANDIDATO	varchar,
                                        NM_URNA_CANDIDATO	varchar,
                                        NM_SOCIAL_CANDIDATO	varchar,
                                        NR_CPF_CANDIDATO	varchar,
                                        NM_EMAIL	varchar,
                                        CD_SITUACAO_CANDIDATURA varchar,	
                                        DS_SITUACAO_CANDIDATURA	varchar,
                                        CD_DETALHE_SITUACAO_CAND varchar,
                                        DS_DETALHE_SITUACAO_CAND varchar,	
                                        TP_AGREMIACAO	varchar,
                                        NR_PARTIDO	varchar,
                                        SG_PARTIDO	varchar,
                                        NM_PARTIDO	varchar,
                                        SQ_COLIGACAO	varchar,
                                        NM_COLIGACAO	varchar,
                                        DS_COMPOSICAO_COLIGACAO	varchar,
                                        CD_NACIONALIDADE	varchar,
                                        DS_NACIONALIDADE	varchar,
                                        SG_UF_NASCIMENTO	varchar,
                                        CD_MUNICIPIO_NASCIMENTO	varchar,
                                        NM_MUNICIPIO_NASCIMENTO	varchar,
                                        DT_NASCIMENTO	varchar,
                                        NR_IDADE_DATA_POSSE	varchar,
                                        NR_TITULO_ELEITORAL_CANDIDATO	varchar,
                                        CD_GENERO	varchar,
                                        DS_GENERO	varchar,
                                        CD_GRAU_INSTRUCAO	varchar,
                                        DS_GRAU_INSTRUCAO	varchar,
                                        CD_ESTADO_CIVIL	varchar,
                                        DS_ESTADO_CIVIL	varchar,
                                        CD_COR_RACA	varchar,
                                        DS_COR_RACA	varchar,
                                        CD_OCUPACAO	varchar,
                                        DS_OCUPACAO	varchar,
                                        VR_DESPESA_MAX_CAMPANHA varchar,	
                                        CD_SIT_TOT_TURNO	varchar,
                                        DS_SIT_TOT_TURNO	varchar,
                                        ST_REELEICAO	varchar,
                                        ST_DECLARAR_BENS	varchar,
                                        NR_PROTOCOLO_CANDIDATURA	varchar,
                                        NR_PROCESSO	varchar,
                                        CD_SITUACAO_CANDIDATO_PLEITO varchar,	
                                        DS_SITUACAO_CANDIDATO_PLEITO	varchar,
                                        CD_SITUACAO_CANDIDATO_URNA	varchar,
                                        DS_SITUACAO_CANDIDATO_URNA	varchar,
                                        ST_CANDIDATO_INSERIDO_URNA varchar
                                        )
                                        """
    create_staging_votes = """create table if not exists raw_data.election_votes (

                            DT_GERACAO varchar,
                            HH_GERACAO varchar,
                            ANO_ELEICAO varchar,
                            CD_TIPO_ELEICAO varchar,
                            NM_TIPO_ELEICAO varchar,
                            NR_TURNO varchar,
                            CD_ELEICAO varchar,
                            DS_ELEICAO varchar,
                            DT_ELEICAO varchar,
                            TP_ABRANGENCIA varchar,
                            SG_UF varchar,
                            SG_UE varchar,
                            NM_UE varchar,
                            CD_MUNICIPIO varchar,
                            NM_MUNICIPIO varchar,
                            NR_ZONA varchar,
                            NR_SECAO varchar,
                            CD_CARGO varchar,
                            DS_CARGO varchar,
                            NR_VOTAVEL varchar,
                            NM_VOTAVEL varchar,
                            QT_VOTOS varchar
                            )
                            """
    create_dim_date = """
                        "date" DATE PRIMARY KEY NOT NULL,
                        year INTEGER NOT NULL,
                        month INTEGER NOT NULL,
                        day INTEGER NOT NULL,
                        day_of_week INTEGER NOT NULL
                       """

    insert_dim_date = """
                        with distinct_date as (
                                select 
                                    to_date(dt_eleicao, 'DD/MM/YYYY') as date
                                from raw_data.election_votes
                                group by 1
                                )
                                
                            select 
                                distinct 
                                    date,
                                    extract(year from date) as year,
                                    extract(month from date) as month,
                                    extract(day from date) as day,
                                    extract(dow from date) as day_of_week
                            from distinct_date
                            
                        """

    create_dim_candidate  = """
                                cpf_number varchar primary key not null,
                                name varchar not null,
                                email varchar,
                                birth_date date,
                                voter_registration_number varchar,
                                marital_status varchar,
                                skin_color varchar,
                                gender varchar,
                                educational_level varchar
                            
                          """

    insert_dim_candidate = """
                            with 

                            candidate_info as (

                                select
                                    distinct 
                                    to_timestamp(dt_geracao || ' ' || hh_geracao , 'DD/MM/YYYY HH:MI:SS') as data_release_date, 
                                    nr_cpf_candidato as cpf_number,
                                    case when
                                        nm_social_candidato != '#NULO#' 
                                        then nm_social_candidato
                                        else nm_candidato 
                                        end as name,
                                    nm_email as email,
                                    to_date(dt_nascimento, 'DD/MM/YYYY') as birth_date,
                                    nr_titulo_eleitoral_candidato as voter_registration_number,
                                    ds_estado_civil as marital_status,
                                    ds_cor_raca as skin_color,
                                    ds_genero as gender,
                                    ds_grau_instrucao as educational_level
                                from raw_data.election_candidates
                                where nr_cpf_candidato != '000000000-4'
                                )
                                
                            , candidate_no_dup as (
                                select 
                                    *,
                                    row_number() over (partition by cpf_number order by data_release_date desc) as row_n
                                from candidate_info
                                )
                                
                            , translation as (
                                select 
                                    cpf_number,
                                    name,
                                    email, 
                                    birth_date,	
                                    voter_registration_number,
                                    case marital_status
                                        when 'CASADO(A)' then 'married'
                                        when 'DIVORCIADO(A)' then 'divorced'
                                        when 'SEPARADO(A) JUDICIALMENTE' then 'legaly_separated'
                                        when 'SOLTEIRO(A)' then 'single'
                                        when 'VIÚVO(A)' then 'widower'
                                    end as marital_status,
                                    case skin_color
                                        when 'AMARELA' then 'yellow'
                                        when 'BRANCA' then 'white'
                                        when 'INDÍGENA' then 'indigenous'
                                        when 'NÃO INFORMADO' then 'not_informed'
                                        when 'PARDA' then 'brown'
                                        when 'PRETA' then 'black'
                                    end as skin_color,
                                    case gender
                                        when 'FEMININO' then 'female'
                                        when 'MASCULINO' then 'male'
                                    end as gender,
                                    case educational_level
                                        when 'ANALFABETO' then 'illiterate'
                                        when 'ENSINO FUNDAMENTAL COMPLETO' then 'complete_elementary_school'
                                        when 'ENSINO FUNDAMENTAL INCOMPLETO' then 'incomplete_elementary_school'
                                        when 'ENSINO MÉDIO COMPLETO' then 'complete_high_school'
                                        when 'ENSINO MÉDIO INCOMPLETO' then 'incomplete_high_school'
                                        when 'LÊ E ESCREVE' then 'reads_and_writes'
                                        when 'SUPERIOR COMPLETO' then 'complete_university'
                                        when 'SUPERIOR INCOMPLETO' then 'incomplete_university'
                                        end as educational_level
                                from candidate_no_dup 
                                where row_n = 1
                                )
                                
                            select * from translation
                            """

    create_dim_election = """   
                        
                            id varchar primary key not null,
                            coverage_type varchar not null,
                            office varchar not null,
                            round integer not null
                        """

    insert_dim_election = """

                            with 
                                election_no_dup as (
                                    select distinct
                                        tp_abrangencia as coverage_type,
                                        ds_cargo as office,
                                        nr_turno::int as round
                                    from raw_data.election_votes
                                )
                                
                            select 
                                md5(coverage_type || '-' || office || '-' || round) as id, 
                                case coverage_type
                                    when 'E' then 'state'
                                    when 'M' then 'municipal'
                                    when 'F' then 'federal'
                                end as coverage_type,
                                case office
                                    when 'DEPUTADO ESTADUAL'
                                        then 'state_congressman'
                                    when 'SENADOR'
                                        then 'senator'
                                    when 'PREFEITO'
                                        then 'mayor'
                                    when 'DEPUTADO FEDERAL'
                                        then 'federal_congressman'
                                    when 'VEREADOR'
                                        then 'city_councilor'
                                    when 'PRESIDENTE'
                                        then 'president'
                                    when 'GOVERNADOR'
                                        then 'governor'
                                    when 'DEPUTADO DISTRITAL'
                                        then 'district_congressman'
                                end as office,
                                round
                            from election_no_dup 
                        
                    """

    create_dim_location = """
                            id varchar primary key not null,
                            electoral_zone integer not null,
                            electoral_section integer not null,
                            city varchar not null,
                            state varchar not null
                            
                        """

    insert_dim_location = """
                            select
                                distinct
                                    md5(nr_zona || '-' || nr_secao || '-' || cd_municipio) as id,
                                    nr_zona::int as electoral_zone,
                                    nr_secao::int as electoral_section,
                                    nm_municipio as city,
                                    sg_uf as state
                                from raw_data.election_votes
                            
                        """

    create_dim_party = """ 
                        id integer PRIMARY KEY NOT NULL,
                        party_name varchar NOT NULL,
                        abbreviation varchar NOT NULL
                        
                    """

    insert_dim_party = """
                        with base_partys as (
                            select 
                                    nr_partido::int as id,
                                    nm_partido as party_name,
                                    sg_partido as abbreviation,
                                    row_number() over (partition by nr_partido order by dt_eleicao desc) as row_n
                            from raw_data.election_candidates 
                            )
                            
                            select 
                                id,
                                party_name,
                                abbreviation
                            from base_partys
                            where row_n = 1
                            """

    create_fact_votes = """ 
                            "date" DATE NOT NULL,
                            location_id varchar NOT NULL,
                            election_id varchar NOT NULL,
                            candidate_id varchar,
                            party_id integer,
                            vote_type varchar not null,
                            vote_amount integer
                        
                        """

    insert_fact_votes  = """
        with 

            base_candidates as (
                select 
                    ano_eleicao,
                    sg_uf,
                    sg_ue,
                    cd_eleicao,
                    nr_candidato,
                    nr_cpf_candidato,
                    cd_cargo
                from raw_data.election_candidates 
                group by 1,2,3,4,5,6,7
            )
                

            , base_votes as (

                select 
                    cd_eleicao,
                    cd_municipio,
                    cd_cargo,
                    nr_votavel,
                    nm_votavel,
                    sg_uf,
                    sg_ue,
                    to_date(dt_eleicao, 'DD/MM/YYYY') as date,
                    md5(nr_zona || '-' || nr_secao || '-' || cd_municipio) as location_id,
                    md5(tp_abrangencia || '-' || ds_cargo || '-' || nr_turno) as election_id,
                    cast(qt_votos as integer) as vote_amount
                from raw_data.election_votes
                )

                select 
                    base_votes.date,
                    base_votes.location_id,
                    base_votes.election_id,
                    base_candidates.nr_cpf_candidato as candidate_id,
                    case
                        when base_candidates.nr_cpf_candidato is null
                            and nr_votavel not in ( '95', '96')
                            then cast(nr_votavel as integer)  
                        end as party_id,
                    case 
                        when base_candidates.nr_cpf_candidato is not null then 'candidate'
                        when nr_votavel = '95' then 'blank_vote'
                        when nr_votavel = '96' then 'null_vote'
                        else 'party'
                        end as vote_type,										
                    base_votes.vote_amount
                    
                from base_votes
                    left join base_candidates
                        on base_candidates.nr_candidato = base_votes.nr_votavel
                        and base_candidates.cd_eleicao = base_votes.cd_eleicao
                        and base_candidates.cd_cargo = base_votes.cd_cargo
                        and base_candidates.sg_uf = base_votes.sg_uf
                        and base_candidates.sg_ue = base_votes.sg_ue
                        
            """
    
    create_dim_city = """
                id varchar primary key,
                city varchar not null,
                state varchar,
                capital boolean,
                residential_population integer,
                brazilian_residential_population integer,
                foreign_residential_population integer,
                household_amount integer,
                urban_household_amount integer,
                rural_household_amount integer
           """
    
    insert_dim_city = """
                select 
                distinct
                md5(city || '-' || state) as id,
                city,
                state,
                capital,
                ibge_res_pop as residential_population,
                ibge_res_pop_bras as brazilian_residential_population,
                ibge_res_pop_estr as foreign_residential_population,
                ibge_du as household_amount,
                ibge_du_urban as urban_household_amount,
                ibge_du_rural as rural_household_amount
            from raw_data.brazil_population
           """


  
   