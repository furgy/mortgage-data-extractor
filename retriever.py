import asyncio
import os
import sys
from playwright.async_api import async_playwright

async def retrieve_statement(account_nickname, date_text, output_dir="statements"):
    async with async_playwright() as p:
        # Using a persistent context allows the user to log in once and keep the session
        user_data_dir = os.path.join(os.getcwd(), ".browser_data")
        context = await p.chromium.launch_persistent_context(user_data_dir, headless=False)
        page = context.pages[0] if context.pages else await context.new_page()

        print(f"Navigating to Huntington...")
        await page.goto("https://www.huntington.com")
        
        # Check if we are logged in or need to log in
        if "onlinebanking.huntington.com" not in page.url:
            print("Please log in manually in the browser window...")
            # Wait for the user to reaching the Hub or Statements page
            await page.wait_for_url("**/AccountServices/Hub/Index", timeout=0)
            print("Login detected!")

        # Navigate to Statements directly if not there
        if "Statements.aspx" not in page.url:
            print("Navigating to Statements page...")
            await page.goto("https://onlinebanking.huntington.com/rol/Statements/Statements.aspx")

        # Find the row for the specified account
        print(f"Looking for account: {account_nickname}")
        rows = await page.query_selector_all("tr")
        target_row = None
        for row in rows:
            text = await row.inner_text()
            if account_nickname in text:
                target_row = row
                break

        if not target_row:
            print(f"Error: Could not find account with nickname '{account_nickname}'")
            await context.close()
            return

        # Find the dropdown in this row
        select_el = await target_row.query_selector("select[id*='clStatementsDdl']")
        if not select_el:
            print(f"Error: Could not find statement dropdown for '{account_nickname}'")
            await context.close()
            return

        # Select the date
        print(f"Selecting date: {date_text}")
        await select_el.select_option(label=date_text)
        
        # Wait for the 'Open' link to appear
        # The link ID usually follows a pattern or is just below/near the select
        open_link = await target_row.wait_for_selector("a[id*='lnkMTGStatement']", timeout=5000)
        if not open_link:
            print("Error: 'Open' link did not appear after selection.")
            await context.close()
            return

        # Setup download listener
        print("Clicking 'Open' and waiting for download...")
        async with page.expect_download() as download_info:
            await open_link.click()
        
        download = await download_info.value
        
        # Save the file
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{account_nickname}_{date_text.replace('/', '').replace(' ', '_')}.pdf"
        filepath = os.path.join(output_dir, filename)
        await download.save_as(filepath)
        
        print(f"Successfully downloaded: {filepath}")
        await context.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: uv run python retriever.py <AccountNickname> <DateText>")
        print("Example: uv run python retriever.py Miller \"12/02/2025 thru 01/02/2026\"")
        sys.exit(1)
    
    nickname = sys.argv[1]
    date = sys.argv[2]
    
    asyncio.run(retrieve_statement(nickname, date))
