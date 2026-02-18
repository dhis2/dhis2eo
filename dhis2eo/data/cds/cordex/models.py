'''
Defines lookup dict of available model combinations for different domains, resolutions, and scenarios.
Structure of MODELS constant is domain.scenario.resolution.list_model_dicts.
'''

MODELS = {
    'africa': {
        '0_22_degree_x_0_22_degree': {
            'rcp_2_6': [
                {'gcm': 'MOHC-HadGEM2-ES', 'rcm': 'CLMcom-KIT-CCLM5-0-15'},
                {'gcm': 'MOHC-HadGEM2-ES', 'rcm': 'GERICS-REMO2015'},
                {'gcm': 'MOHC-HadGEM2-ES', 'rcm': 'ICTP-RegCM4-7'},
                {'gcm': 'MPI-M-MPI-ESM-MR', 'rcm': 'ICTP-RegCM4-7'},
                {'gcm': 'NCC-NorESM1-M', 'rcm': 'CLMcom-KIT-CCLM5-0-15'},
                {'gcm': 'NCC-NorESM1-M', 'rcm': 'GERICS-REMO2015'},
                {'gcm': 'NCC-NorESM1-M', 'rcm': 'ICTP-RegCM4-7'},
                {'gcm': 'MPI-M-MPI-ESM-LR', 'rcm': 'CLMcom-KIT-CCLM5-0-15'},
                {'gcm': 'MPI-M-MPI-ESM-LR', 'rcm': 'GERICS-REMO2015'},
            ],
            'rcp_4_5': [
                {'gcm': 'CCCma-CanESM2', 'rcm': 'CCCma-CanRCM4'},
            ],
            'rcp_8_5': [
                {'gcm': 'CCCma-CanESM2', 'rcm': 'CCCma-CanRCM4'},
                {'gcm': 'MOHC-HadGEM2-ES', 'rcm': 'CLMcom-KIT-CCLM5-0-15'},
                {'gcm': 'MOHC-HadGEM2-ES', 'rcm': 'GERICS-REMO2015'},
                {'gcm': 'MOHC-HadGEM2-ES', 'rcm': 'ICTP-RegCM4-7'},
                {'gcm': 'MPI-M-MPI-ESM-MR', 'rcm': 'ICTP-RegCM4-7'},
                {'gcm': 'NCC-NorESM1-M', 'rcm': 'CLMcom-KIT-CCLM5-0-15'},
                {'gcm': 'NCC-NorESM1-M', 'rcm': 'GERICS-REMO2015'},
                {'gcm': 'NCC-NorESM1-M', 'rcm': 'ICTP-RegCM4-7'},
                {'gcm': 'MPI-M-MPI-ESM-LR', 'rcm': 'CLMcom-KIT-CCLM5-0-15'},
                {'gcm': 'MPI-M-MPI-ESM-LR', 'rcm': 'GERICS-REMO2015'},
            ],
        }
    }
}
