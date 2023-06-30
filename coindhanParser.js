const fs = require("fs");
const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

// Read the source CSV file
fs.createReadStream("source.csv")
  .pipe(csv())
  .on("data", (row) => {
    // Extract the "Market" value
    const marketValue = row.Market;

    // Process "Market" value to create "Asset Sent(SYMBOL)" and "Asset Received(SYMBOL)"
    let assetSent = "";
    let assetReceived = "";

    if (marketValue.endsWith("inr")) {
      assetSent = marketValue.slice(0, -3);
      assetReceived = "INR";
    } else {
      assetReceived = marketValue;
    }

    // Extract the "Fee" value
    const feeValue = row.Fee;

    // Process "Fee" value to create "Quantity of Fee Paid" and "Fee Paid In(SYMBOL)"
    let quantityFeePaid = "";
    let feePaidInSymbol = "";

    if (feeValue !== "0") {
      const feeMatch = feeValue.match(/(\d+(\.\d+)?)(\s*)([a-zA-Z]+)/);

      if (feeMatch && feeMatch.length === 5) {
        quantityFeePaid = feeMatch[1];
        feePaidInSymbol = feeMatch[4];
      }
    }

    // Extract the "TDS" value
    const tdsValue = row.TDS;

    // Extract the "Date (in UTC)" value
    const dateUTCValue = row["Date (in UTC)"];

    // Process "Date (in UTC)" value to create "Date" and "Time"
    const dateTimeParts = dateUTCValue.split(" ");
    const dateValue = dateTimeParts[0];
    const timeValue = dateTimeParts[1];

    // Map column names and values
    const newRow = {
      Date: dateValue,
      Time: timeValue,
      "Type of Transaction": getTypeOfTransaction(row.Side),
      Description: row["Order Type"],
      "Asset Sent(SYMBOL)": assetSent,
      "Asset Received(SYMBOL)": assetReceived,
      "Fee Paid In(SYMBOL)": feePaidInSymbol,
      "Quantity of Fee Paid": quantityFeePaid,
      "Quantity of Tds Paid": tdsValue,
    };

    // Append the mapped row to the new CSV file
    csvWriter.writeRecords([newRow]);
  })
  .on("end", () => {
    console.log("CSV file successfully processed.");
  });

// Create the new CSV file with mapped columns
const csvWriter = createCsvWriter({
  path: "mapped.csv",
  header: [
    { id: "Date", title: "Date" },
    { id: "Time", title: "Time" },
    { id: "Type of Transaction", title: "Type of Transaction" },
    { id: "Description", title: "Description" },
    { id: "Asset Sent(SYMBOL)", title: "Asset Sent(SYMBOL)" },
    { id: "Asset Received(SYMBOL)", title: "Asset Received(SYMBOL)" },
    { id: "Fee Paid In(SYMBOL)", title: "Fee Paid In(SYMBOL)" },
    { id: "Quantity of Fee Paid", title: "Quantity of Fee Paid" },
    { id: "Quantity of Tds Paid", title: "Quantity of Tds Paid" },
  ],
});

// Helper function to determine the type of transaction based on the "Side" value
function getTypeOfTransaction(side) {
  if (side.toLowerCase() === "buy") {
    return "Buy with INR";
  } else if (side.toLowerCase() === "sell") {
    return "Sell with INR";
  } else {
    return "";
  }
}
