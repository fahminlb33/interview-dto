import pandas as pd

from dagster import AssetExecutionContext, MetadataValue, MaterializeResult, asset

from .resources import GoogleSheetResource

@asset()
def pull_request_form(gsheet: GoogleSheetResource) -> MaterializeResult:
    df = gsheet.read(0)
    df.to_csv('data/request_form.csv', index=False)

    return MaterializeResult(
        metadata={
            "path": "data/request_form.csv",
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

@asset(deps=[pull_request_form])
def push_approval(gsheet: GoogleSheetResource) -> MaterializeResult:
    df = pd.read_csv('data/request_form.csv')
    gsheet.write(1, df)

    return MaterializeResult(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

@asset(deps=[push_approval])
def pull_approval(gsheet: GoogleSheetResource) -> MaterializeResult:
    df = gsheet.read(1)
    df.to_csv('data/approval.csv', index=False)

    return MaterializeResult(
        metadata={
            "path": "data/approval.csv",
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

@asset()
def pull_users(gsheet: GoogleSheetResource) -> MaterializeResult:
    df = gsheet.read(2)
    df.to_csv('data/users.csv', index=False)

    return MaterializeResult(
        metadata={
            "path": "data/users.csv",
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

@asset()
def pull_roles(gsheet: GoogleSheetResource) -> MaterializeResult:
    df = gsheet.read(3)
    df.to_csv('data/roles.csv', index=False)

    return MaterializeResult(
        metadata={
            "path": "data/roles.csv",
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

@asset(deps=[pull_approval, pull_roles, pull_users])
def push_grant_log(context: AssetExecutionContext) -> MaterializeResult:
    # get today date
    today = pd.to_datetime('today')

    # get all relevant data
    df_approval = pd.read_csv('data/approval.csv', parse_dates=['Timestamp'], dayfirst=True)
    df_users = pd.read_csv('data/users.csv')
    df_roles = pd.read_csv('data/roles.csv')

    # merge all
    df = df_approval.merge(df_users, on='Email', how='inner')
    df = df.merge(df_roles, left_on="Role", right_on="Kode", how='inner')

    # normalize column names
    df = df.drop(columns=["Nama_y"])
    df = df.rename(columns={"Nama_x": "Nama"})
    df.columns = df.columns.str.lower().str.replace(r' ', '_')

    # calculate elapsed day
    df["elapsed"] = (today - df["timestamp"]).dt.days
    df["is_expired"] = df["elapsed"] > df["durasi_akses"].str.replace('d', '').astype(int)

    # phase 1: grant access
    for user in df[~df["is_expired"]].itertuples():
        context.log.info(f"Granting access to {user.email} with role {user.role} for {user.durasi_akses}")
        # TODO: grant access
    
    # phase 2: revoke expired access
    for user in df[df["is_expired"]].itertuples():
        context.log.info(f"Revoking access to {user.email} with role {user.role} for {user.durasi_akses}")
        # TODO: revoke access
    
    df.to_csv('data/grant_log.csv', index=False)

    return MaterializeResult(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )
