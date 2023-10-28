{{
    config(
        unique_key='LGA_CODE'
    )
}}

with
source as (
    select * from "postgres"."raw"."censusgone"
),
lgacode as (
    select LGA_CODE_2016 as LGA_CODE_SOURCE, CAST(SUBSTRING(LGA_CODE_2016 FROM 4) AS NUMERIC) AS LGA_CODE
    from source
),
combined as (
    select source.*, lgacode.LGA_CODE as LGA_CODE
    from source
    LEFT JOIN lgacode
    ON source.LGA_CODE_2016 = lgacode.LGA_CODE_SOURCE
),
cleaned as (
select combined.* -- Exclude the columns used in the join
from combined
WHERE combined.LGA_CODE IS NOT NULL -- Filter out rows with missing LGA_CODE
),
unknown as (
    select
        'UNKNOWN' as LGA_CODE_2016,
        0 as Tot_P_M,
        0 as Tot_P_F,
        0 as Tot_P_P,
        0 as Age_0_4_yr_M,
        0 as Age_0_4_yr_F,
        0 as Age_0_4_yr_P,
        0 as Age_5_14_yr_M,
        0 as Age_5_14_yr_F,
        0 as Age_5_14_yr_P,
        0 as Age_15_19_yr_M,
        0 as Age_15_19_yr_F,
        0 as Age_15_19_yr_P,
        0 as Age_20_24_yr_M,
        0 as Age_20_24_yr_F,
        0 as Age_20_24_yr_P,
        0 as Age_25_34_yr_M,
        0 as Age_25_34_yr_F,
        0 as Age_25_34_yr_P,
        0 as Age_35_44_yr_M,
        0 as Age_35_44_yr_F,
        0 as Age_35_44_yr_P,
        0 as Age_45_54_yr_M,
        0 as Age_45_54_yr_F,
        0 as Age_45_54_yr_P,
        0 as Age_55_64_yr_M,
        0 as Age_55_64_yr_F,
        0 as Age_55_64_yr_P,
        0 as Age_65_74_yr_M,
        0 as Age_65_74_yr_F,
        0 as Age_65_74_yr_P,
        0 as Age_75_84_yr_M,
        0 as Age_75_84_yr_F,
        0 as Age_75_84_yr_P,
        0 as Age_85ov_M,
        0 as Age_85ov_F,
        0 as Age_85ov_P,
        0 as Counted_Census_Night_home_M,
        0 as Counted_Census_Night_home_F,
        0 as Counted_Census_Night_home_P,
        0 as Count_Census_Nt_Ewhere_Aust_M,
        0 as Count_Census_Nt_Ewhere_Aust_F,
        0 as Count_Census_Nt_Ewhere_Aust_P,
        0 as Indigenous_psns_Aboriginal_M,
        0 as Indigenous_psns_Aboriginal_F,
        0 as Indigenous_psns_Aboriginal_P,
        0 as Indig_psns_Torres_Strait_Is_M,
        0 as Indig_psns_Torres_Strait_Is_F,
        0 as Indig_psns_Torres_Strait_Is_P,
        0 as Indig_Bth_Abor_Torres_St_Is_M,
        0 as Indig_Bth_Abor_Torres_St_Is_F,
        0 as Indig_Bth_Abor_Torres_St_Is_P,
        0 as Indigenous_P_Tot_M,
        0 as Indigenous_P_Tot_F,
        0 as Indigenous_P_Tot_P,
        0 as Birthplace_Australia_M,
        0 as Birthplace_Australia_F,
        0 as Birthplace_Australia_P,
        0 as Birthplace_Elsewhere_M,
        0 as Birthplace_Elsewhere_F,
        0 as Birthplace_Elsewhere_P,
        0 as Lang_spoken_home_Eng_only_M,
        0 as Lang_spoken_home_Eng_only_F,
        0 as Lang_spoken_home_Eng_only_P,
        0 as Lang_spoken_home_Oth_Lang_M,
        0 as Lang_spoken_home_Oth_Lang_F,
        0 as Lang_spoken_home_Oth_Lang_P,
        0 as Australian_citizen_M,
        0 as Australian_citizen_F,
        0 as Australian_citizen_P,
        0 as Age_psns_att_educ_inst_0_4_M,
        0 as Age_psns_att_educ_inst_0_4_F,
        0 as Age_psns_att_educ_inst_0_4_P,
        0 as Age_psns_att_educ_inst_5_14_M,
        0 as Age_psns_att_educ_inst_5_14_F,
        0 as Age_psns_att_educ_inst_5_14_P,
        0 as Age_psns_att_edu_inst_15_19_M,
        0 as Age_psns_att_edu_inst_15_19_F,
        0 as Age_psns_att_edu_inst_15_19_P,
        0 as Age_psns_att_edu_inst_20_24_M,
        0 as Age_psns_att_edu_inst_20_24_F,
        0 as Age_psns_att_edu_inst_20_24_P,
        0 as Age_psns_att_edu_inst_25_ov_M,
        0 as Age_psns_att_edu_inst_25_ov_F,
        0 as Age_psns_att_edu_inst_25_ov_P,
        0 as High_yr_schl_comp_Yr_12_eq_M,
        0 as High_yr_schl_comp_Yr_12_eq_F,
        0 as High_yr_schl_comp_Yr_12_eq_P,
        0 as High_yr_schl_comp_Yr_11_eq_M,
        0 as High_yr_schl_comp_Yr_11_eq_F,
        0 as High_yr_schl_comp_Yr_11_eq_P,
        0 as High_yr_schl_comp_Yr_10_eq_M,
        0 as High_yr_schl_comp_Yr_10_eq_F,
        0 as High_yr_schl_comp_Yr_10_eq_P,
        0 as High_yr_schl_comp_Yr_9_eq_M,
        0 as High_yr_schl_comp_Yr_9_eq_F,
        0 as High_yr_schl_comp_Yr_9_eq_P,
        0 as High_yr_schl_comp_Yr_8_belw_M,
        0 as High_yr_schl_comp_Yr_8_belw_F,
        0 as High_yr_schl_comp_Yr_8_belw_P,
        0 as High_yr_schl_comp_D_n_g_sch_M,
        0 as High_yr_schl_comp_D_n_g_sch_F,
        0 as High_yr_schl_comp_D_n_g_sch_P,
        0 as Count_psns_occ_priv_dwgs_M,
        0 as Count_psns_occ_priv_dwgs_F,
        0 as Count_psns_occ_priv_dwgs_P,
        0 as Count_Persons_other_dwgs_M,
        0 as Count_Persons_other_dwgs_F,
        0 as Count_Persons_other_dwgs_P,
        0 as LGA_CODE
)

select * from cleaned
union all
select * from unknown