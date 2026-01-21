# Full manual - Sport Prediction Project

## Table of Contents
- [Vocabulary](#vocabulary)
- [Prediction game rules](#gamerules)
- [Input tools and accounts](#inputtoolsaccounts)
- [Input Parameters](#inputparameters)
- [Current sources](#currentsources)
- [DropBox repository tree](#dropboxtree)
- [Files required](#filesrequired)
- [How to add competition to scope](#addtoscope)
- [Calendar management and automatic run](#calendar)
- [Usage - Entry points](#usage)
- [Modifying output_need_manual file](#modifyingoutputneedmanual)
- [Automatic email sent](#emailsent)

## Vocabulary<a name="vocabulary"></a>

Vocabulary used in the rules and in the software

- **source**: A source is external websites to which the program connects:  
  - to get games to predict details (sport league source)
  - to get messages prediction, and post templates and results (forums sources)
- **season**: A season is a specific sport league during a year from a specific country (ex: The Elite league of basketball in France in 2024-2025)
- **competition**: A competition is a part of a season where rules are consistent (ex: Regular Season, or PlayOffs)
- **gameday**: A gameday is a set of games from a competition grouped together, where each team can play only once (ex: Gameday 3),usually designed by the sport league organization. Usually, all games of a gameday occur within the same time frame.
- **player**: A player is a forum user participating in the prediction game. Players must have a valid account on the forum, in order to post their predictions freely.
- **software administrator**: The person responsible for maintaining the software and the game.  
  This includes updating input files, adding competitions to the scope, checking player messages, and validating results.

## Prediction game rules<a name="gamerules"></a>
This part present the prediction games rules. It's a free game on forum with no rewards except recognition from other players.

### How to participate

Every gameday, players can make predictions on all games of the gameday, or a part of them, before their start time. Predictions posted after are ignored.

- Estimation of score difference

    Players predict the score difference (first team score - second team score, positive or negative) by copying this template, and replacing the '+1' with their prediction.
    ```
        #GameDayID.ForumGameID# Team1 vs Team2 ==> +1
    ```
    The template is part of the gameday prediction template, prealably automatically posted by the software thanks to the [planned calendar of run](#calendar).  
    An exemple (in French) can be found under GitHub directory: *file_exemples/forumoutput_inited_s2_13emejournee_france.txt*    
    Team1 is considered to be the home team, Team2 is considered to be the away team by the software, even if the game is played at neutral venue.

- Bonus game

    Players may select a bonus game amongst the one they predicted, by copying this template and replacing 'GameDayID.ForumGameID' with their choice.    
    ``` 
        #GameDayID.BN# Bonus game => GameDayID.ForumGameID
    ```
    The template is also part of the gameday prediction template, prealably posted by the software thanks to the [planned calendar of run](#calendar).    
    An exemple (in French) is available: *file_exemples/forumoutput_inited_s2_13emejournee_france.txt*  
    The bonus will awards players 3 times more points than a normal prediction

- Choice of a team for the prediction championship

    Before a defined deadline, players can choose and change their team for the prediction championship (see below), copying this template, and changing the chosen team.  
    ``` 
        SEASON.TM# Team for prediction championship => Chosen team
    ``` 
    The template is also part of the gameday prediction template, prealably posted by the software thanks to the [planned calendar of run](#calendar).    
    An exemple (in French): *file_exemples/forumoutput_inited_s2_13emejournee_france.txt*  
    The deadline is defined by the software administrator with the column SEASON_TEAMCHOICE_DEADLINE of [the input required manual file season.csv](#season)

### Individual competition

Players compete individually based on the number of points earned.

- "Good winner" score

    If players found the good winner, they get 15 points. Else 0.  
    Their score is multiplied by 3 if it is the bonus game.

- "Good score difference" score

    If players found the exact score difference they get 30 points. Else 15 points - the difference between the result and the prediction.  
    Their score is multiplied by 3 if it is the bonus game.

- "Automatic" score

    If players followed the template for their prediction they get 1 point.  
    If not, the software administrator have to insert manually the prediction in [the input required manual file correction_prediction.csv](#correctionprediction), and players receives 0 point.   
    Their score is multiplied by 3 if it is the bonus game.

- Gameday Number of points

    For each player, the gameday number of points is the sum of scores for all predicted games related to the gameday:
    - Good winner score  
    - Good score difference score  
    - Automatic score  
    
- Gameday individual ranking

    Players are ranked in descending order of their gameday number of points.

- Season individual ranking

    Players are ranked by the sum of their gameday points across the entire season. 
        
    Some KPI are included:  
    - Number of gameday with at least one prediction
    - Total number of predictions made
    - Number of gamedays where the player ranked first

- Average ranking

    The number of season point is divided by the number of prediction made.
    Players which made strictly more tham 50% of predictions in the season are ranked by the descending result

### Prediction championship

Players can choose or change their team amongst predicted league teams, until a deadline fixed by the software administrator with the column SEASON_TEAMCHOICE_DEADLINE of [the input required manual file season.csv](#season).  
Their individual points contribute to the chosen team, for the prediction championship, until the end of the season, unless changed before the deadline.

- Games in the prediction championship

    During the competition of the regular season, games are assumed to be the same than the ones of the predicted league.   
    This is decided by the software administrator with the column IS_SAME_FOR_PREDICTCHAMP of [the input required manual file competition.csv](#competition).  
    For other competitions, depending on the ranking of the prediction championship, their schedule might be different. For exemple for playoffs, the qualified TOP8 will probably be different than the one from the predicted league.
    In this case, the software administrator have to write manual prediction championship games in [the input required manual file predictchamp_game_to_add.csv](#predictchampgametoadd)

- Teams  points

    For each game:
    - A team’s score is the maximum gameday individual score among all players who selected that team.
    - The home team receives a 20% bonus.
    - If the game is at a neutral venue, the administrator may disable the home advantage using HAS_HOME_ADV in  
    [predictchamp_game_to_add.csv](#predictchampgametoadd).

- Prediction championship game results:

    For each game of the prediction championship gameday, the team with most points win.   
    If tied, the home team win.  
    If home-away format deactivated, the first listed team wins (considered at home by the program).

- Prediction championship ranking

    A ranking between teams wins and losses is calculated.

### MVP election [Most Valuable Predictor]

For the last gameday of each month—or the last gameday of a competition—the program posts the results along with the total points earned during that period for each player (both individual and team-based).  
An election for the MVP takes place based on these total points earned.

## Input tools and accounts<a name="inputtoolsaccounts"></a>

The software relies on several external tools, services, and accounts to operate correctly.  
See *README.md* section for more details.

## Input Parameters<a name="inputparameters"></a>

The software uses a set of environment variables as input, provided as GitHub secrets, or before run for local running:
- **SNOWFLAKE_USERNAME** and **SNOWFLAKE_PASSWORD** to log in Snowflake account
- **RCLONE_CONFIG_BASE64**, Base64‑encoded rclone configuration used to authenticate to the Dropbox account.
- **IMGBB_API_KEY**, API key to log to imgbb account
- **GMAIL_USER** and **GMAIL_APP_PASSWORD**, email adress and password to send [automatic email on the status of run](#emailsent)
- **RECIPIENT_EMAIL**, comma-separated list of email adress for recipient of the [email automatically sent on the status of run](#emailsent)

Moreover, considering the [current sources](#currentsources), stored as GitHub secrets, or before run for local running:
- **LNB_URL** : the LNB website url
- **BI_URL**, the BI forum website, **BI_USERNAME** and **BI_PASSWORD** to log to BI acccount and post results

Considering the context of run:
- <a name="isoutputauto"></a>**IS_OUTPUT_AUTO** (0/1): If 1, the output_need file will be generated during run from the [planned calendar](#calendar). If 0, it uses the [output_need_manual file](#outputneedmanual), which can be [edited prealably by the software administrator](#modifyingoutputneedmanual)
- **IS_TESTRUN** (0/1): If 1, the program runs in test environment. If 0 it runs in production environment

## Current sources<a name="currentsources"></a>
The software currently processes seasons from one source named "LNB" (French Elite basketball).  
These LNB seasons are handled through the module *get_game_details_lnb.py*, which retrieves game schedules, results, and competition metadata.

The software reads predictions and post templates and results on one forum, a PhpBB style forum named "BI" (French forum), with several topics (one for predictions per season, one for result per season).  
These BI topics are handled through the module *get_messages_details_bi.py*.  

At this stage, French is the only current supported language of the scope.  

The software administrator can [add more sources](#addtoscope)

## DropBox repository tree<a name="dropboxtree"></a>
To run successfully, the program requires some files and folders from DropBox, organized in a stable tree.  
This tree can be changed by modifying both [paths file](#pathsfile) and Python parameters in *config.py*

```
prediction_files/
├── docs/
│   ├── # Contain documentation of the software and some essential documents files
│       # (see Files Required section)
└── global_manual_inputs/ 
│   ├── # Manual inputs (see Files Required section) shared by Prod and Test environment 
│       # Downloaded for both Prod or Test runs
├── Prod/ # Folder for production environment files
│   ├── local_manual_inputs/
│   │   ├── # Manual inputs (see Files Required section) 
│   │       # related to the production environment run
│   │       # they will be download when running in Prod
│   ├── current/ # Files related to the most recent software run
│   │   ├── inputs/ 
│   │   │   ├── manual/ 
│   │   │   │   ├── # Manual input files used for the last run
│   │   │   ├── calculated/ 
│   │   │   │   ├── # Files calculated at the beginning of the run 
│   │   │   │       # Used downstream during execution 
│   │   │   │       # Some of them must be created initially (see Files Required section)
│   │   ├── outputs/
│   │   │   ├── database/ 
│   │   │   │   ├── # CSV files representing Snowflake tables after the last run 
│   │   │   │       # Used for backup or to initialize a new Snowflake account
│   │   │   ├── python/ 
│   │   │   │   ├── # Contains files calculated by python program. 
│   │   │   │       # Some of them must be created initially (see Files Required section)
│   │   │   ├── captured/ 
│   │   │   │   ├── # Contains result jpg capture, which are posted on forums
│   │   │   ├── post/ 
│   │   │   │   ├── # Contains message posted on forums in txt files
│   ├── -1/ # Files from the previous run (backup)
│   │   ├── ... # Same tree than current/
│   ├── -2/ # Files from two runs ago (backup)
│   │   ├── ... # Same tree than current/
│   ├── -3/ # Files from three runs ago (backup)
│   │   ├── ... # Same tree than current/
├── Test/ # Folder for test environment files
│   ├── ... # Same tree than Prod/
```

## Files required<a name="filesrequired"></a>
Some files must be written manually as input of the program, others at least created and initialized for the software to run. 
Example of each file is copied under *file_exemples/* folder

### Documents files required, in DropBox *docs/* folder:

- <a name="pathsfile"></a>**paths.csv**. Defines where each file is located on DropBox, if they need to be downloaded, and which filter to apply on their content. It can be change if changing Python parameters in *config.py* and changing [DropBox tree](#dropboxtree).
    - **NAME**: Short identifier used by the program (Python creates `str_NAME` or `df_NAME` objects).  
    - **PATH**: Full Dropbox path to the file.  
    - **IS_ENCAPSULATED** (0/1): Whether the file must be decapsulated before being converted into a Python object.  
    - **IS_FOR_UPLOAD** (0/1): Whether the file should be uploaded back to Dropbox after the run.  
    - **FILTERING_CATEGORY**: Defines when filtering applies (categories defined in *config.py*).  
    - **FILTERING_FILE**: The NAME of the file used as the filtering reference.  
    - **FILTERING_COLUMN**: Columns from the filtering file used to match values.  
    - **DOWNLOAD_CATEGORY**: Defines when the file must be downloaded (see *config.py*).  
    - **PYTHON_CATEGORY**: Defines when the file should be uploaded to Snowflake via Python.  
    - **DBT_CATEGORY**: Defines when the related Snowflake table should be updated via DBT.

- **Trophy.jpg**  
    Image used to generate the graphical playoff bracket for the prediction championship.  
    (See [generate playoffs](#generateplayoffs) for details.)

- <a name="playoffstable"></a>**playoffs_table.txt**: 
    Contains Python arrays representing playoff bracket positions.  
    Used to generate the graphical playoff image posted on forums.
    (See [generate playoffs](#generateplayoffs) for details.)
    
### Manual files required (either in global_manual_inputs - if used both in Prod and Test-  or local_manual_inputs):
They must be created and updated by the software administrator, according to the scope or evolutions.

- <a name="season"></a>**season.csv**: Defines the seasons included in the scope.
    - **SEASON_ID**: an unique id for the season
    - **SEASON_SPORT** : the label of the sport of the season
    - **SEASON_COUNTRY** : The country of the season
    - **SEASON_NAME** : The name of the season
    - **SEASON_DIVISION** : The league of the season
    - **SEASON_TEAMCHOICE_DEADLINE** (date) : The limit date for the player to choose/change its team for the prediction championship 

- <a name="competition"></a>**competition.csv**: Defines competitions belonging to each season from [season.csv](#season)
    - **SEASON_ID**: the id of season - same as on [season.csv](#season)/SEASON_ID
    - **COMPETITION_ID**: The id of the competition
    - **COMPETITION_LABEL**: The long label of the competition
    - **COMPETITION_SOURCE**: The source of games for the competition. The program will call the python module *get_game_details_COMPETITION_SOURCE*
    - **COMPETITION_SOURCE_ID**: The id of the competition on the source for filtering
    - **IS_SAME_FOR_PREDICTCHAMP** (0/1): Boolean telling if the predition championship games will be the same as the one from the prediction league or not.  
    If not, the software administrator must add manual games to [predictchamp_game_to_add.csv](#predictchampgametoadd)
    - **IS_TO_LOAD** (0/1): Boolean telling if it needs to load the competition on the database while running ["Init compet"](#initcompet) or not

- <a name="topic"></a>**topic.csv**: Defines forum topics used for predictions and results.
    - **SEASON_ID**: the id of season corresponding to the topic - same as on [season.csv](#season)/SEASON_ID
    - **FORUM_SOURCE**: The source of topic. The program will call the python module *get_messages_details_FORUM_SOURCE*
    - **FORUM_COUNTRY**: The country of the forum:
        - to get the timezone for prediction, and check if prediction was before the beginning of the related game
        - for publishing prediction templates, and results with the good language 
    - **TOPIC_NUMBER** (int): The number of the topic of the forum source
    - **IS_FOR_PREDICT** (0/1): Boolean to define if the topic is for reading prediction and post templates or not
    - **IS_FOR_RESULT** (0/1): Boolean to define if the topic is for result publication or not

- **snowflake_account_connect.csv**: Defines Snowflake connection parameters (except credentials, [stored as GitHub Secrets](#inputparameters)).
    - **ACCOUNT**: The name of the account
    - **WAREHOUSE**: The name of the warehouse used to run DML
    - **DATABASE_PROD**: The name of the database for production
    - **DATABASE_TEST**: The name of the database for test  

- <a name="correctionprediction"></a>**correction_prediction.csv**: Used to overwrite prediction or choices (bonus or team) from a player (if malformed or not recognised by the program).
    - **FORUM_SOURCE**: The forum source - same as on [topic.csv](#topic)/FORUM_SOURCE
    - **MESSAGE_FORUM_ID** (int): The number of the message in the forum source where the bad prediction/ choice where made
    - **PREDICT_ID**: The identification of the prediction or choice to overwrite
    - **PREDICT**: The value of the prediction to overwrite
    - **IS_PROGRAM_REFINEMENT** (0/1): Boolean telling if it is a correction due to badly written message or program refinement.  
    If program refinement, players will have points for AUTOMATIC_SCORE, else they won't.
    
- <a name="gamedaymodification"></a>**gameday_modification.csv**: Used to overwrite a source gameday name or group several gameday under the same name, for calculations
    - **SEASON_ID** : the id of the season - same as on [season.csv](#season)/SEASON_ID
    - **COMPETITION_ID**: the id of the competition - same as on [competition.csv](#competition)/COMPETITION_ID
    - **GAMEDAY**: The source gameday name
    - **GAMEDAY_MODIFIED**: The name of the gameday after modification

- <a name="outputneedmanual"></a>**output_need_manual.csv**: If ran automatically (**IS_OUTPUT_AUTO = 1**), the program will calculate output need based on run [calendar planned](#calendar). 
    If ran manually (**IS_OUTPUT_AUTO = 0**), the program will use this file as output need
    - **TASK_RUN**: The [type of the task](#taskrun) ran 
    - **SEASON_ID**: The id of the season - same as on [season.csv](#season)/SEASON_ID
    - **SEASON_SPORT**: The name of the sport  - same as on [season.csv](#season)/SEASON_SPORT. Can be left empty
    - **SEASON_COUNTRY**: The name of the season country - same as on [season.csv](#season)/SEASON_COUNTRY. Can be left empty
    - **SEASON_NAME**: The name of the season - same as on [season.csv](#season)/SEASON_NAME. Can be left empty
    - **SEASON_DIVISION**: The division of the season - same as on [season.csv](#season)/SEASON_DIVISION. Can be left empty
    - **COMPETITION_ID**: The id of the competition ran - same as on [competition.csv](#competition)/COMPETITION_ID.
    - **GAMEDAY**: The gameday name to run in the related competition, AFTER the potential modification on [gameday_modification.csv](#gamedaymodification)
    - **TS_TASK_UTC**: The utc task timestamp as on run [calendar planned](#calendar).
    - **TS_TASK_LOCAL**: The local season task timestamp as on run [calendar planned](#calendar). Can be left empty
    - **IS_TO_INIT**(0/1): Boolean telling to calculate and publish prediction gameday template
    - **IS_TO_CALCULATE**(0/1): Boolean telling to calculate and publish results message of the gameday (see [here](#istocalculate) for more details) 
    - **IS_TO_DELETE**(0/1): Boolean telling to delete the results of the gameday on database (see [here](#istodelete) for more details) 
    - **IS_TO_RECALCULATE**(0/1): Boolean telling to recalculate the results of the gameday on database and publish results (see [here](#istorecalculate) for more details) 
    - **MESSAGE_ACTION** (CHECK/RUN/AVOID): Telling which actions to do with messages (see [here](#messageaction) for more details)
    - **GAME_ACTION** (RUN/AVOID): Telling which actions to do with games (see [here](#gameaction) for more details)

- <a name="predictchampgametoadd"></a>**predictchamp_game_to_add.csv**: 
Defines manual prediction championship games when IS_SAME_FOR_PREDICTCHAMP = 0 from a competition in [competition.csv](#competition)  
    - **PREDICTCHAMP_GAME_ID**: A unique incremental id, through all seasons
    - **SEASON_ID**: The id of the season - same as on [season.csv](#season)/SEASON_ID
    - **GAMEDAY**: The gameday name on which the prediction championship game is added, BEFORE the potential modification on [gameday_modification.csv](#gamedaymodification)
    - **TEAM_HOME**: The name of the home team, or first team if not in a home-away format
    - **TEAM_AWAY**: The name of the away team, or second team if not in a home-away format
    - **HAS_HOME_ADV** (0/1): Boolean telling to add home bonus points when calculate results or not

- **game_modification.csv**: The program creates a list of games from the source with an ID (from 1 to N, see the file *curated_game.sql* for more details).  
This ID is displayed on the forum - as "GameDayID.ForumGameID", see an exemple in the file *file_exemples/forumoutput_inited_s2_13emejournee_france.txt* -. This ID is then used to match player prediction with the good game on the database.   
The game_modification file is used to overwrite the ForumGameID displayed to players:
    - **SEASON_ID**: the id of the season - same as on [season.csv](#season)/SEASON_ID
    - **GAME_SOURCE_ID**: The id on the game source
    - **GAME_FORUM_ID**: The id to overwrite for the forum  (The ForumGameID part on the display)

- **message_quote_to_keep.csv**: Some forums authorize quoting from another message. This file defines message where quotes content should be preserved (if player predictions are in it)
    - **FORUM_SOURCE**: The source of the forum - same as on [topic.csv](#topic)/FORUM_SOURCE
    - **MESSAGE_FORUM_ID**: The number of the message on the forum source where to keep quote

- <a name="booleancheckmessagemanually"></a>**boolean_check_message_manually.csv**: Define decision to check messages before running calculations or not. 
    - **SEASON_ID**: the id of the season - same as on [season.csv](#season)/SEASON_ID
    - **BOOLEAN_CHECK_MESSAGE_MANUALLY** (0/1): If 1, then the software administrator will need to validate messages before the program to run calculations. Else no.

- <a name="scriptcreatingdatabase"></a>**script_creating_database.txt**: Full SQL script used to [initialize a snowflake account](#initsnowflake). Parameters between `#` are replaced during runtime.

- <a name="inittemplate"></a>**output_gameday_init_template_xxxx.txt**: Template for gameday initialization messages (prediction templates).  
A version must exist for each forum language, replacing xxxx with the country
It is personalized during run (removing parameters between #).  
An example of message posted based on it, is also copied in *files_example*, named *forumoutput_inited_s2_13emejournee_france.txt*

- <a name="calculationtemplate"></a>**output_gameday_calculation_template_xxxx.txt**: Template for gameday result messages.  
A version must exist for each forum language, replacing xxxx with the country
It is personalized during run (removing parameters between #).  
An example of message posted based on it, is also copied in *files_example*, named *forumoutput_calculated_s2_12emejournee_france.txt*  

### Semi-automatic input files  
The following files are downloaded, modified by the program then uploaded back to DropBox. They must be created once with the correct headers.

- **RUN_TYPE.csv**, in *current/inputs/calculated*: Store the type of run for each run, log initiation then termination of each run.  
    - **RUN_TIME_UTC**: Will store the UTC time of run
    - **EVENT**: Will store the event: "initiate" at the beginning of run / "terminate" at the end with success
    - **RUN_TYPE**: Will store which module is ran.
    - **RUN_METHOD**: Will store if run is automatic (schedule) or manual (workflow_dispatch) from GitHub Actions, or empty if ran locally
    - **OUTPUT_AUTO**: Will store the value of the parameter [IS_OUTPUT_AUTO](#isoutputauto)
    - **PLANNED_RUN_TIME_UTC**: Will store the utc run time [planned in the calendar](#calendar), if ran automatically

- **task_done.csv**<a name="taskdone"></a>, in *current/outputs/python*: Tracks tasks already ran. The program will compare it with the [planned calendar](#calendar) to know what remains to be run, and when.
    - **TASK_RUN**: The [type of the task](#taskrun)
    - **SEASON_ID**: The id of the season - same as on [season.csv](#season)/SEASON_ID	
    - **GAMEDAY**: The gameday name	
    - **TS_TASK_UTC**: The time of [planned run](#calendar) in UTC

- <a name="nextruntimeutc"></a>**next_run_time_utc.txt**, in *current/outputs/python*: Store the next run time utc according to the [planned calendar](#calendar).   
When creating it must store the value "NONE". The calendar and its value will be updated after [adding new seasons and competitions in the scope](#addtoscope), according to new [planned calendar](#calendar)

- <a name="messagecheckts"></a>**message_check_ts.csv**, in *current/outputs/python*: Stores the timestamp of the last manual message check. (messages before won't be loaded). It will be helpful for the program to know if there are [new messages to validate before running calculation](#messageaction):
    - **SEASON_ID**: The id of the related season - same as on [season.csv](#season)/SEASON_ID
    - **LAST_CHECK_TS_UTC**: The timestamp of the last time messages have been manually checked in UTC time  
If the [boolean_check_message_manually](#booleancheckmessagemanually) is 0, the modification of message check ts will be automatic.

## How to add competition to the scope<a name="addtoscope"></a>

The software administrator can add to the scope any competion from any sport league they want, as long as games cannot end with a tie score; and link it to any forum they want, in any language:
- To prepare a new competition

    If the competition is on a new source, the software administrator must:
    - develop a Python module *get_game_details_xxx.py* with xxx being the source of the season.
    - add the module in the Python dictionary *game_info_functions* of the module *game_actions.py*, for it to be called
    - add URL of the source in GitHub secret to call it in the software [ -> See section Input parameters](#inputparameters)

    Then if the season doesn't exist yet, the software administrator must:
    - add the season to [the input required manual file season.csv](#season)

    Finally the software administrator must:  
    - add the new competition to [the input required manual file competition.csv](#competition) with IS_TO_LOAD = 1 
    - possibly modify other [required files](#filesrequired) according to modification the software administrator wants to apply to sources

- To link the competition with new topics

    If the topic is on a new forum, the software administrator must:
    - develop a Python module *get_messages_details_xxx.py* with xxx being the source of the forum.
    - add the module in the Python dictionary *messages_info_functions* of the module *message_actions.py*, for it to be called
    - add URL of the forum in GitHub secret to call it in the software [ -> See section Input parameters](#inputparameters)
    
    If the forum language is with a new language, the software administrator must:
    - add new files to DropBox: [output_gameday_init_template_xxxx.txt](#inittemplate) and [output_gameday_calculation_template_xxxx.txt](#calculationtemplate) with xxxx being the new language
    - add the new language to the json file *output_actions/output_actions_translations.json* by adding translations for messages parameters personalized during run

    Finally the software administrator must:  
    - add the new topics to [the input required manual file topic.csv](#topic) with possibly modifying other required manual files

- To add the new competition and topics to the database, the software administrator must:
    - run [the "Init compet" entry point](#initcompet)

## Calendar management and automatic run <a name="calendar"></a>

The software can be run manually by the software administrator, using an [output_need_manual file](#outputneedmanual) that they prealably [modified](#modifyingoutputneedmanual).  
But the software is also ran automatically, every 12 minutes, by GitHub using GitHub Actions, the file ouput_need being generated automatically. 
The automatic run of the software relies on the calendar management, to know what to run, and when.

During the [main run](#mainrun) or ["Init compet" run](#initcompet), DBT performs DML operations on database:  

For each gameday of the season, the begin datetime of the first game, and the begin datetime of the last game is calculated.  
Considering them, the view *VW_CALENDAR* (see the file *vw_calendar.sql* for more details) enables the program to know when to perform tasks for each gameday (for more details of the definition of tasks see [here](#taskrun)). 
- TASK_RUN = 'UPDATEGAMES': planned several times before the begin datetime of the first game of the gameday to detect possible changes of date and times of games
- TASK_RUN = 'INIT': planned at the beginning of the first game of the previous gameday - it will post the prediction template for the new gameday
- TASK_RUN = 'CHECK': planned at the beginning of each game, to read the latest games predictions from players
- TASK_RUN = 'CALCULATE': planned two hours after the beginning of last game of the gameday, to perform and post calculations. If the last game is not finished yet, the extraction will return an error (see the module *get_game_details_lnb.py*), and wait for the next automatic run to retry again.

After updating the database without errors, the software will insert the ran task into [task_done.csv file](#taskdone).  
It will then compare the calendar of run to it, to get what utc time will be run next, and insert it into [next_run_time_utc.txt](#nextruntimeutc).
    
Next automatic run by GitHub actions, at the very beginning of the run, the software will check if current utc time greater than the time from [#next_run_time_utc.txt](nextruntimeutc).  
If it is not, it stops running, and [no email is sent](#emailsent). 
Else it continues, generating output_need related to the task from the calendar and downstreams. 

## Usage - Entry points<a name="usage"></a>
The program can be run locally or through GitHub Actions. There are four entry points.
- <a name="initsnowflake"></a>Init snowflake: Creates two new databases (production and test) on a snowflake account, populating tables with csv files from Dropbox folder *current/outputs/database/*
    - Can be run locally: 
        ```
            cd PYTHON_PREDICT
            python exe_init_snowflake.py
        ```
    - Can be run through GitHub actions through the workflow *gitrun_init_snowflake.yml*

- <a name="initcompet"></a>Init compet: [Add a new competition](#addtoscope) to the scope of the program (production database and test database) with its own games and calculting its [planned calendar](#calendar). The file [next_run_time_utc.txt](nextruntimeutc) is updated based on this calendar.  
The input file [competition.csv](#competition) must have a row with this new competition with IS_TO_LOAD = 1  
    - Can be run locally: 
        ```
            cd PYTHON_PREDICT
            python exe_init_compet.py 
        ```
    - Can be run through GitHub actions through the workflow *gitrun_init_compet.yml*

- <a name="generateplayoffs"></a>Generate playoffs table: Creates a jpg graphical picture for prediction championship results, according to values in [the input file playoffs_table.txt](#playoffstable).  
The playoffs competition must exist in the database through the file [competition.csv](#competition). (see [here](#addtoscope) for adding it)
    - Can be run locally:  
        ```
            cd PYTHON_PREDICT
            python exe_playoffs_table.py
        ```
    - Can be run through GitHub actions through the workflow *gitrun_playoffs_table.py*

- <a name="mainrun"></a>Main run: Run the program on a daily basis, to read and post message, read games and calculate software results, based on the [planned calendar](#calendar)
    - Can be run locally:
        ```
            cd PYTHON_PREDICT
            python exe_main.py
        ```
    - Can be run through GitHub actions through different workflows:
        - *gitrun_main_auto_prod.yml*  
            Runs in production with output_need calculated automatically based on the [calendar of run](#calendar).  
                - Can be triggered manually through worflow_dispatch  
                - Scheduled to run automatically every 12 minutes  
        - *gitrun_main_manual_prod.yml*  
            Runs in production with output_need got from the file [output_need_manual.csv file](#outputneedmanual)
        - *gitrun_main_manual_test.yml*  
            Runs in test with output_need got from the file [output_need_manual.csv file](#outputneedmanual)

## Modifying output_need_manual file<a name="modifyingoutputneedmanual"></a>

The program runs tasks automatically based on [the planned calendar](#calendar). It can be overwrite by running a specific task with [the input file output_need_manual](#outputneedmanual). Following is presented the different tasks processed by the software:

- <a name="gameaction"></a>GAME_ACTION
    - 'RUN': Will get games from the source with the specified GAMEDAY and update the database with details (date, time, results)
    - 'AVOID': Won't extract and update games details on the database

- <a name="messageaction"></a>MESSAGE_ACTION
    - <a name="messageactioncheck"></a>'CHECK': Will get messages from topics related with the SEASON_ID defined, from the utc datetime found in [the file message_check_ts.csv](#messagecheckts), update the database with predictions, but won't make calculation yet
    - 'RUN': Will get messages from topics related with the SEASON_ID defined, from the utc datetime indicated in message_check_ts.csv, until the beginning of the last game of the specified GAMEDAY
        - If there are messages, and the [boolean_check_message_manually](#booleancheckmessagemanually) is 1, the task is converted into MESSAGE_ACTION = 'CHECK' and don't make calculation.
            The software administrator will have to validate them and change [the file message_check_ts.csv](#messagecheckts) in order to run calculations.  
            If ran automatically, the program will indefinitely convert the task until messages are validated
        - If there are no new messages (ie no messages not validated yet), or the [boolean_check_message_manually](#booleancheckmessagemanually) is 0, it will perform calculations.
    - 'AVOID': won't get messages

- <a name="taskrun"></a>TASK_RUN
    - 'INIT': Will initiate the defined GAMEDAY by posting the template of predictions on the forum
    - 'UPDATEGAMES': if combined with GAME_ACTION = 'RUN', will update details about games about the specified GAMEDAY in the database, looking for possible change in date and time
    - 'CHECK': if associated with MESSAGE_ACTION = 'CHECK', retreives messages for the software administrator validation
    - 'CALCULATE': Post GAMEDAY results message

- <a name="istocalculate"></a>IS_TO_CALCULATE: If 1, will perform calculation on the database for the desired GAMEDAY if not already done.

- <a name="istodelete"></a>IS_TO_DELETE: If 1, will delete calculations on the database for the desired GAMEDAY (scores = 0)

- <a name="istorecalculate"></a>IS_TO_RECALCULATE: If 1, will recalculate by deleting and calculating again the desired GAMEDAY on the database

## Automatic email sent<a name="emailsent"></a>

After running [an entry point](#usage) through GitHub actions, GitHub will send an email to the recipient - [see Input parameters](#inputparameters) - to inform them of the success or failure.  
The mail of success of the [main entry point](#mainrun) will be sent with copy of:  
- output_need file text (#outputneed) automatically or manually generated
- next run time utc file text (#nextruntimeutc)
- the sql query to validate players prediction, for a [CHECK run](#messageactioncheck). 
If ran by GitHub action automatically without meeting any task of  [the planned calendar](#calendar), no email will be sent.
An exemple of email sent can be found in **files_examples**



