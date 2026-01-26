// warm-cache.js
const redis = require('redis');
const mariadb = require('mariadb');
require('dotenv').config();

async function warmCache() {
  console.log('🔥 Starting cache warm-up...');
  
  // Connect to Redis
  const redisClient = redis.createClient({
    url: process.env.REDIS_URL
  });
  await redisClient.connect();
  console.log('✅ Redis connected');
  
  // Connect to MariaDB
  const pool = mariadb.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    port: process.env.DB_PORT,
    database: process.env.DB_NAME || 'Goodreads',
    connectionLimit: 10
  });
  
  const conn = await pool.getConnection();
  console.log('✅ MariaDB connected');
  
  // Get popular books
  console.log('📚 Fetching popular books from Goodreads DB...');
  const rows = await conn.query(
    'SELECT isbn, star_rating, num_ratings FROM Scrape WHERE num_ratings > 1000 ORDER BY num_ratings DESC LIMIT 150000'
  );
  console.log(`📚 Found ${rows.length} books to cache`);
  
  // Cache them
  let cached = 0;
  for (const row of rows) {
    if (row.isbn) {
      await redisClient.setEx(
        `goodreads:${row.isbn}`,
        86400 * 90, // 90 days
        JSON.stringify({
          rating: parseFloat(row.star_rating) || 0,
          ratingsCount: parseInt(row.num_ratings) || 0,
          source: 'goodreads'
        })
      );
      cached++;
      
      if (cached % 10000 === 0) {
        console.log(`✅ Cached ${cached} / ${rows.length} books`);
      }
    }
  }
  
  console.log(`🎉 Cache warm-up complete! Cached ${cached} books`);
  
  conn.release();
  await pool.end();
  await redisClient.quit();
  process.exit(0);
}

warmCache().catch(err => {
  console.error('❌ Cache warm-up failed:', err);
  process.exit(1);
});
