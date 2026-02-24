const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");

const app = express();
app.use(cors()); // Allow cross-origin requests from frontend
app.use(express.json());

// MySQL connection pool
const pool = mysql.createPool({
  host: "localhost",
  user: "lab6",
  password: "lab6",
  database: "dsci560_wells",
  waitForConnections: true,
  connectionLimit: 10,
});

// ---------- routes ----------
// Health check endpoint (for testing if server is running)
app.get("/api/health", (req, res) => {
  res.json({ ok: true, message: "API is running" });
});

// GET /api/wells - List all wells for map markers
// Returns well_id, api_number, name (alias for well_name), latitude, longitude
// Only includes wells with valid coordinates
app.get("/api/wells", async (req, res) => {
  try {
    const [rows] = await pool.query(`
      SELECT
        well_id,
        api_number,
        well_name AS name,
        latitude,
        longitude
      FROM wells
      WHERE latitude IS NOT NULL
        AND longitude IS NOT NULL
        AND latitude <> ''
        AND longitude <> ''
    `);

    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// GET /api/wells/:id - Fetch full well detail for popup
// Returns well record + all associated stimulation records
app.get("/api/wells/:id", async (req, res) => {
  try {
    const id = req.params.id;

    // Fetch well by ID
    const [[well]] = await pool.query(
      "SELECT * FROM wells WHERE well_id = ?",
      [id]
    );

    if (!well) {
      return res.status(404).json({ error: "well not found" });
    }

    // Fetch all stimulations for this well
    const [stimulations] = await pool.query(
      "SELECT * FROM stimulations WHERE well_id = ? ORDER BY stimulation_id ASC",
      [id]
    );

    res.json({ well, stimulations });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// Start server on port 3000
app.listen(3000, () => {
  console.log("API running at http://localhost:3000");
});