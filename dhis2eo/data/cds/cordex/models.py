'''
Defines lookup dict of available model combinations for different domains, resolutions, and scenarios.
Structure of MODELS constant is domain.scenario.resolution.list_model_dicts.
'''

MODELS = {
    'africa': {
        '0_22_degree_x_0_22_degree': {
            'rcp_2_6': [
                {'gcm': 'MOHC-HadGEM2-ES', 'rcm': 'CLMcom-KIT-CCLM5-0-15', 'ens': 'r1i1p1'},
                {'gcm': 'MOHC-HadGEM2-ES', 'rcm': 'GERICS-REMO2015', 'ens': 'r1i1p1'},
                {'gcm': 'MOHC-HadGEM2-ES', 'rcm': 'ICTP-RegCM4-7', 'ens': 'r1i1p1'},
                {'gcm': 'MPI-M-MPI-ESM-MR', 'rcm': 'ICTP-RegCM4-7', 'ens': 'r1i1p1'},
                {'gcm': 'NCC-NorESM1-M', 'rcm': 'CLMcom-KIT-CCLM5-0-15', 'ens': 'r1i1p1'},
                {'gcm': 'NCC-NorESM1-M', 'rcm': 'GERICS-REMO2015', 'ens': 'r1i1p1'},
                {'gcm': 'NCC-NorESM1-M', 'rcm': 'ICTP-RegCM4-7', 'ens': 'r1i1p1'},
                {'gcm': 'MPI-M-MPI-ESM-LR', 'rcm': 'CLMcom-KIT-CCLM5-0-15', 'ens': 'r1i1p1'},
                {'gcm': 'MPI-M-MPI-ESM-LR', 'rcm': 'GERICS-REMO2015', 'ens': 'r1i1p1'},
            ],
            'rcp_4_5': [
                {'gcm': 'CCCma-CanESM2', 'rcm': 'CCCma-CanRCM4', 'ens': 'r1i1p1'},
            ],
            'rcp_8_5': [
                {'gcm': 'CCCma-CanESM2', 'rcm': 'CCCma-CanRCM4', 'ens': 'r1i1p1'},
                {'gcm': 'MOHC-HadGEM2-ES', 'rcm': 'CLMcom-KIT-CCLM5-0-15', 'ens': 'r1i1p1'},
                {'gcm': 'MOHC-HadGEM2-ES', 'rcm': 'GERICS-REMO2015', 'ens': 'r1i1p1'},
                {'gcm': 'MOHC-HadGEM2-ES', 'rcm': 'ICTP-RegCM4-7', 'ens': 'r1i1p1'},
                {'gcm': 'MPI-M-MPI-ESM-MR', 'rcm': 'ICTP-RegCM4-7', 'ens': 'r1i1p1'},
                {'gcm': 'NCC-NorESM1-M', 'rcm': 'CLMcom-KIT-CCLM5-0-15', 'ens': 'r1i1p1'},
                {'gcm': 'NCC-NorESM1-M', 'rcm': 'GERICS-REMO2015', 'ens': 'r1i1p1'},
                {'gcm': 'NCC-NorESM1-M', 'rcm': 'ICTP-RegCM4-7', 'ens': 'r1i1p1'},
                {'gcm': 'MPI-M-MPI-ESM-LR', 'rcm': 'CLMcom-KIT-CCLM5-0-15', 'ens': 'r1i1p1'},
                {'gcm': 'MPI-M-MPI-ESM-LR', 'rcm': 'GERICS-REMO2015', 'ens': 'r1i1p1'},
            ],
        }
    },
    'europe': {
        '0_11_degree_x_0_11_degree': {
            'rcp_2_6': [
                {'gcm': 'ICHEC-EC-EARTH', 'rcm': 'GERICS-REMO2015', 'ens': 'r12i1p1'},
                {'gcm': 'ICHEC-EC-EARTH', 'rcm': 'SMHI-RCA4', 'ens': 'r12i1p1'},
                {'gcm': 'ICHEC-EC-EARTH', 'rcm': 'CLMcom-CLM-CCLM4-8-17', 'ens': 'r12i1p1'},
                {'gcm': 'ICHEC-EC-EARTH', 'rcm': 'DMI-HIRHAM5', 'ens': 'r3i1p1'},
                {'gcm': 'ICHEC-EC-EARTH', 'rcm': 'KNMI-RACMO22E', 'ens': 'r12i1p1'},
                {'gcm': 'ICHEC-EC-EARTH', 'rcm': 'MOHC-HadREM3-GA7-05', 'ens': 'r12i1p1'},

                {'gcm': 'GERICS-REMO2015', 'rcm': 'GERICS-REMO2015', 'ens': 'r1i1p1'},
                {'gcm': 'GERICS-REMO2015', 'rcm': 'ICTP-RegCM4-6', 'ens': 'r1i1p1'},
                {'gcm': 'GERICS-REMO2015', 'rcm': 'SMHI-RCA4', 'ens': 'r1i1p1'},
                {'gcm': 'GERICS-REMO2015', 'rcm': 'DMI-HIRHAM5', 'ens': 'r1i1p1'},
                {'gcm': 'GERICS-REMO2015', 'rcm': 'KNMI-RACMO22E', 'ens': 'r1i1p1'},
                {'gcm': 'GERICS-REMO2015', 'rcm': 'MOHC-HadREM3-GA7-05', 'ens': 'r1i1p1'},
            ],
        }
    },
}
