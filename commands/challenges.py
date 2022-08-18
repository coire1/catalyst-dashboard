import typer
import pandas as pd
import json

import utils.async_command
from api.ideascale import ideascaleApi

app = typer.Typer()

@app.command()
def criteria_score(
    input_file: str = typer.Option("", help="Valid assessments CSV path to import."),
    withdrawals_file: str = typer.Option("", help="Proposals withdrawn CSV"),
    output_file: str = typer.Option("", help="CSV path to export.")
):
    """
    Generate challenge specific stats for criteria.
    """

    withdrawals = pd.read_csv(withdrawals_file)
    withdrawals_ids = list(withdrawals['proposal_id'])

    assessments = pd.read_csv(input_file)
    assessments = assessments[~assessments['proposal_id'].isin(withdrawals_ids)]
    assessments['Final Rating'] = assessments[
        ['Impact / Alignment Rating', 'Feasibility Rating', 'Auditability Rating']
    ].mean(axis=1)
    by_challenge = assessments.groupby('Challenge')

    by_proposals = assessments.groupby(['proposal_id', 'Idea Title', 'Challenge'])
    d2 = by_proposals['Final Rating'].mean().round(2)
    b_results = d2.to_frame(name='Final Rating').reset_index()
    by_challenge_proposals = b_results.groupby('Challenge')

    b_results = pd.DataFrame()
    b_results['AVG Overall Score'] = by_challenge_proposals['Final Rating'].mean().round(2)

    results = pd.DataFrame()

    # Avg obtained by first grouping by proposals and obtaining proposal mean
    results['AVG Overall Score'] = by_challenge_proposals['Final Rating'].mean().round(2)
    # Avg obtained by the mean of all assessments in a challenge
    results['AVG Overall Score (raw assessments)'] = by_challenge['Final Rating'].mean().round(2)
    results['AVG Impact'] = by_challenge['Impact / Alignment Rating'].mean().round(2)
    results['AVG Feasibility'] = by_challenge['Feasibility Rating'].mean().round(2)
    results['AVG Auditability'] = by_challenge['Auditability Rating'].mean().round(2)

    print(results)
    results = results.transpose()
    results.to_csv(output_file)

@app.command()
def proposals_score(
    input_file: str = typer.Option("", help="Proposals scores tab."),
    withdrawals_file: str = typer.Option("", help="Proposals withdrawn CSV"),
    output_file: str = typer.Option("", help="CSV path to export.")
):
    """
    Generate challenge specific stats for proposals.
    """

    withdrawals = pd.read_csv(withdrawals_file)
    withdrawals_ids = list(withdrawals['proposal_id'])

    proposals = pd.read_csv(input_file)
    proposals = proposals[~proposals['proposal_id'].isin(withdrawals_ids)]

    results = pd.DataFrame()


    by_challenge = proposals.groupby('Challenge', group_keys=False)
    top_10 = by_challenge.apply(
        lambda x : x.sort_values(by = 'Rating Given', ascending = False)
        .head(int(round(len(x)*0.10)))
    ).groupby('Challenge')

    by_challenge = proposals.groupby('Challenge', group_keys=False)
    top_20 = by_challenge.apply(
        lambda x : x.sort_values(by = 'Rating Given', ascending = False)
        .head(int(round(len(x)*0.20)))
    ).groupby('Challenge')

    results['Top 10%'] = top_10['Rating Given'].mean().round(2)
    results['Top 20%'] = top_20['Rating Given'].mean().round(2)

    by_challenge = proposals.groupby('Challenge')
    results['Highest'] = by_challenge['Rating Given'].max()
    results['Lowest'] = by_challenge['Rating Given'].min()

    results = results.transpose()
    results.to_csv(output_file)

@app.async_command()
async def health_check(
    assessments_file: str = typer.Option("", help="Valid assessments CSV"),
    proposals_file: str = typer.Option("", help="Proposals score CSV"),
    challenges_map: str = typer.Option("", help="Challenges map (title-id)"),
    withdrawals_file: str = typer.Option("", help="Proposals withdrawn CSV"),
    output_file: str = typer.Option("", help="CSV path to export.")
):

    withdrawals = pd.read_csv(withdrawals_file)
    withdrawals_ids = list(withdrawals['proposal_id'])

    challenges = json.load(open(challenges_map))
    assessments = pd.read_csv(assessments_file)
    assessments = assessments[~assessments['proposal_id'].isin(withdrawals_ids)]
    proposals = pd.read_csv(proposals_file)
    proposals = proposals[~proposals['proposal_id'].isin(withdrawals_ids)]
    by_challenge = assessments.groupby('Challenge')
    proposals_by_challenge = proposals.groupby('Challenge', group_keys=False)

    top_10 = proposals_by_challenge.apply(
        lambda x : x.sort_values(by = 'Rating Given', ascending = False)
        .head(int(round(len(x)*0.10)))
    ).groupby('Challenge')

    results = pd.DataFrame()
    results['CA Reviews (valid)'] = by_challenge['Impact / Alignment Rating'].count()
    results['Insights'] = 0
    results['Ideas'] = 0
    results['Proposals'] = 0
    results['Comments'] = 0

    for index, row in results.iterrows():
        challenge = next((item for item in challenges if item['title'] == index), None)
        if (challenge):
            fullChallenge = await ideascaleApi.get_campaign_by_id(challenge['id'])
            comments_count = fullChallenge['commentCount']
            stats = fullChallenge['stageStatistics']
            for stat in stats:
                if (stat['label'] == 'Insight sharing reserve'):
                    insights_count = stat['ideaCount']
                if (stat['label'] == 'Archive'):
                    archive_count = stat['ideaCount']
                if (stat['label'] == 'Governance phase'):
                    governance_count = stat['ideaCount']
            ideas_count = archive_count + governance_count
            row['Comments'] = comments_count
            row['Insights'] = insights_count
            row['Ideas'] = ideas_count
            row['Proposals'] = governance_count

    results['Top 10%'] = top_10['Rating Given'].mean().round(2)

    print(results)
    results = results.transpose()
    results.to_csv(output_file)

@app.async_command()
async def challenge_specific_funded(
    challenges_map: str = typer.Option("", help="Challenges map (title-id)"),
    proposals_file: str = typer.Option("", help="Proposals score CSV"),
    voting_results: str = typer.Option("", help="Merged voting results."),
    output_file: str = typer.Option("", help="CSV path to export."),
):

    challenges = json.load(open(challenges_map))
    voting_results = pd.read_csv(voting_results)
    voting_results.rename(columns = {'proposal_id': 'chain_proposal_id'}, inplace=True)
    proposals = pd.read_csv(proposals_file)

    proposals.set_index('proposal_id', inplace=True)
    voting_results.set_index('internal_id', inplace=True)
    proposals = proposals.join(voting_results, rsuffix='_voting').reset_index()

    proposals = proposals[proposals['status'] == 'FUNDED']

    by_challenge = proposals.groupby('Challenge')

    results = pd.DataFrame()

    results['Count'] = by_challenge['Rating Given'].count().astype(int)
    results['High'] = by_challenge['Rating Given'].max().round(2)
    results['Low'] = by_challenge['Rating Given'].min().round(2)
    results['AVG Score'] = by_challenge['Rating Given'].mean().round(2)

    results = results.transpose()

    results.to_csv(output_file)
    print(results)

@app.async_command()
async def fund_stats_funded(
    proposals_file: str = typer.Option("", help="Proposals score CSV"),
    voting_results: str = typer.Option("", help="Merged voting results."),
    challenge_settings_title: str = typer.Option("", help="Challenge setting title")
):

    voting_results = pd.read_csv(voting_results)
    voting_results.rename(columns = {'proposal_id': 'chain_proposal_id'}, inplace=True)
    proposals = pd.read_csv(proposals_file)

    proposals.set_index('proposal_id', inplace=True)
    voting_results.set_index('internal_id', inplace=True)
    proposals = proposals.join(voting_results, rsuffix='_voting').reset_index()

    funded = proposals[proposals['status'] == 'FUNDED']
    funded_challenge_setting = funded[funded['Challenge'] == challenge_settings_title]
    funded_regular = funded[funded['Challenge'] != challenge_settings_title]
    approved = proposals[proposals['meets_approval_threshold'] == 'YES']

    print(f"Proposals - approved: {len(approved)}")
    print(f"Proposals - funded (all): {len(funded)}")
    print(f"Proposals - funded (regular): {len(funded_regular)}")
    print(f"Proposals - funded (challenge setting): {len(funded_challenge_setting)}")



@app.async_command()
async def innovation_baseline(
    proposals_file: str = typer.Option("", help="Proposals score CSV"),
    challenges_map: str = typer.Option("", help="Challenges map (title-id)"),
    withdrawals_file: str = typer.Option("", help="Proposals withdrawn CSV"),
    output_file: str = typer.Option("", help="CSV path to export."),
    governance_stage: str= typer.Option("Assess QA", help="Label for the governance stage.")
):

    withdrawals = pd.read_csv(withdrawals_file)
    withdrawals_ids = list(withdrawals['proposal_id'])

    challenges = json.load(open(challenges_map))
    proposals = pd.read_csv(proposals_file)
    proposals = proposals[~proposals['proposal_id'].isin(withdrawals_ids)]

    results = pd.DataFrame()
    results['Ingishts (total)'] = 0
    results['Ideas (total)'] = 0
    results['Ideas (archived)'] = 0
    results['Proposals (voting app)'] = 0
    results['Proposers (unique)'] = 0
    results['Co-proposers (unique)'] = 0
    results['Proposers + Co-proposers (unique)'] = 0
    results['Total Ask (voting app)'] = 0

    for challenge in challenges:
        row = {}
        print(f"Requesting challenge {challenge['title']}...")
        fullChallenge = await ideascaleApi.get_campaign_by_id(challenge['id'])
        proposals = await ideascaleApi.get_proposals_by_campaign_id(challenge['id'])
        print(f"All proposals: {len(proposals)}")
        funds = 0
        proposers = []
        coproposers = []
        count_active_proposals = 0
        for proposal in proposals:
            if (proposal['stageLabel'] == governance_stage):
                count_active_proposals = count_active_proposals + 1
                funds = funds + int(proposal['customFieldsByKey']['requested_funds'])
                author = proposal['authorId']
                if (author not in proposers):
                    proposers.append(author)
                contributors = proposal['contributors']
                for contributor in contributors:
                    contributor_id = contributor['id']
                    if (contributor_id not in proposers):
                        coproposers.append(contributor_id)
        print(f"All active proposals: {count_active_proposals}")
        proposers = list(set(proposers))
        coproposers = list(set(coproposers))
        all_proposers = list(set(proposers + coproposers))
        comments_count = fullChallenge['commentCount']
        stats = fullChallenge['stageStatistics']
        for stat in stats:
            if (stat['label'] == 'Insight sharing reserve'):
                insights_count = stat['ideaCount']
            if (stat['label'] == 'Archive'):
                archive_count = stat['ideaCount']
            if (stat['label'] == governance_stage):
                governance_count = stat['ideaCount']
        ideas_count = archive_count + governance_count
        row['Ingishts (total)'] = insights_count
        row['Ideas (total)'] = ideas_count
        row['Ideas (archived)'] = archive_count
        row['Proposals (voting app)'] = governance_count
        row['Proposers (unique)'] = len(proposers)
        row['Co-proposers (unique)'] = len(coproposers)
        row['Proposers + Co-proposers (unique)'] = len(all_proposers)
        row['Total Ask (voting app)'] = funds
        results.loc[challenge['title']] = row

    print(results)
    results = results.transpose()
    results.to_csv(output_file)

@app.command()
def assign_challenges(
    input_file: str = typer.Option("", help="Input file"),
    proposals_file: str = typer.Option("", help="Proposals JSON"),
    challenges_file: str = typer.Option("", help="Challenges JSON"),
    output_file: str = typer.Option("", help="CSV path to export.")
):
    proposals = json.load(open(proposals_file))
    challenges = json.load(open(challenges_file))
    entities = pd.read_csv(open(input_file))
    entities['Challenge'] = ''

    for index, row in entities.iterrows():
        proposal = next((item for item in proposals if item['id'] == row['proposal_id']), None)
        if proposal:
            challenge = next((item for item in challenges if item['id'] == proposal['category']), None)
            if (challenge):
                entities.at[index, 'Challenge'] = challenge['title']
            else:
                print('challenge not found')
        else:
            print(row)
            print('not found')

    entities.to_csv(output_file, index=False)

@app.async_command()
async def fund_stats(
    group_id: str = typer.Option("", help="challenges group id"),
):

    fund = await ideascaleApi.get_campaign_by_group(group_id)
    tot_active = 0
    tot_archived = 0
    tot_insight_sharing = 0
    for challenge in fund[0]['campaigns']:
        stats = challenge['stageStatistics']
        for stat in stats:
            if (stat['label'] == 'Assess QA'):
                tot_active = tot_active + stat['ideaCount']
            if (stat['label'] == 'Archive'):
                tot_archived = tot_archived + stat['ideaCount']
            if (stat['label'] == 'Insight sharing reserve'):
                tot_insight_sharing = tot_insight_sharing + stat['ideaCount']

    tot_submission = tot_active + tot_archived
    print(f"Total submissions: {tot_submission}")
    print(f"Total archived: {tot_archived}")
    print(f"Total insights: {tot_insight_sharing}")
    print(f"Total active: {tot_active}")
