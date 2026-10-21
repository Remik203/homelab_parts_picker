import sqlite3

conn = sqlite3.connect("../data/listings.db")
cursor = conn.cursor()

print("Database connected successfully")

# Table for all listings from different portals
cursor.execute("""
    CREATE TABLE IF NOT EXISTS listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        platform VARCHAR(50),           -- 'OLX', 'Allegro'
        url TEXT UNIQUE,
        title TEXT,
        price DECIMAL(10, 2),           -- Cena za CAŁE ogłoszenie
        raw_description TEXT,           -- Surowy tekst dla Ollamy
        seller_location VARCHAR(100),   -- Ważne przy odbiorze osobistym
        is_active BOOLEAN DEFAULT 1,    -- Czy ogłoszenie nadal wisi
        scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
""")

# Table with components from each listings
cursor.execute("""
    CREATE TABLE IF NOT EXISTS cpu (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id INTEGER REFERENCES listings(id) ON DELETE CASCADE,
        manufacturer VARCHAR(50),       -- 'Intel', 'AMD'
        model VARCHAR(100),             -- np. 'Ryzen 5 3600'
        generation INTEGER,             -- np. 6, 8, 12
        socket VARCHAR(50),             -- 'AM4', 'LGA1151'
        cores INTEGER,
        threads INTEGER,
        base_clock_ghz DECIMAL(4, 2),
        boost_clock_ghz DECIMAL(4, 2),
        has_igpu BOOLEAN,               -- Kluczowe dla homelabu bez dedykowanej karty!
        tdp_w INTEGER,                  -- Ważne przy dobieraniu zasilacza
        passmark_score INTEGER          -- Z bazy offline
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS motherboard (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id INTEGER REFERENCES listings(id) ON DELETE CASCADE,
        manufacturer VARCHAR(50),
        model VARCHAR(100),
        form_factor VARCHAR(50),        -- 'ATX', 'mATX', 'ITX'
        socket VARCHAR(50),             -- Musi pasować do cpu.socket
        chipset VARCHAR(50),            -- np. 'B450', 'Z390'
        ram_type VARCHAR(20),           -- 'DDR3', 'DDR4', 'DDR5'
        ram_slots INTEGER,              -- Ilość gniazd
        max_ram_gb INTEGER,
        m2_nvme_slots INTEGER,          -- Ilość portów pod szybkie dyski
        sata_ports INTEGER,
        rear_io_ports TEXT              -- JSON, np. '{"rj45": 1, "usb3": 4, "hdmi": 1}'
    )
""")
cursor.execute(""" 
    CREATE TABLE IF NOT EXISTS ram (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id INTEGER REFERENCES listings(id) ON DELETE CASCADE,
        manufacturer VARCHAR(50),
        model VARCHAR(100),
        type VARCHAR(20),               -- 'DDR3', 'DDR4', 'DDR5'
        form_factor VARCHAR(50),        -- 'UDIMM' (PC), 'SODIMM' (Laptop/MiniPC)
        is_ecc BOOLEAN,                 -- TRUE/FALSE
        capacity_gb INTEGER,            -- Łączna pojemność z ogłoszenia
        modules_count INTEGER,          -- np. 2 (co oznacza zestaw 2x8GB przy capacity=16)
        speed_mhz INTEGER,              -- np. 3200
        cas_latency INTEGER             -- CL, np. 16
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS storage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id INTEGER REFERENCES listings(id) ON DELETE CASCADE,
        manufacturer VARCHAR(50),
        model VARCHAR(100),
        type VARCHAR(50),               -- 'SSD', 'HDD'
        interface VARCHAR(50),          -- 'SATA', 'NVMe', 'SAS'
        form_factor VARCHAR(50),        -- '2.5', '3.5', 'M.2'
        capacity_gb INTEGER,
        health_status TEXT              -- Często wrzucają screeny z CrystalDiskInfo
    )
""")
cursor.execute(""" 
    CREATE TABLE IF NOT EXISTS gpu (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id INTEGER REFERENCES listings(id) ON DELETE CASCADE,
        manufacturer VARCHAR(50),       -- 'ASUS', 'MSI'
        chipset_vendor VARCHAR(50),     -- 'NVIDIA', 'AMD', 'Intel'
        model VARCHAR(100),             -- 'RTX 3060'
        vram_gb INTEGER,
        vram_type VARCHAR(20),          -- 'GDDR6'
        tdp_w INTEGER
    )
""")
cursor.execute("""    
    CREATE TABLE IF NOT EXISTS psu (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id INTEGER REFERENCES listings(id) ON DELETE CASCADE,
        manufacturer VARCHAR(50),
        model VARCHAR(100),
        wattage INTEGER,
        form_factor VARCHAR(50),        -- 'ATX', 'SFX'
        efficiency_rating VARCHAR(50),  -- '80+ Bronze', '80+ Gold'
        is_modular BOOLEAN
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS prebuilt_systems (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id INTEGER REFERENCES listings(id) ON DELETE CASCADE,
        system_type VARCHAR(50),        -- 'MiniPC', 'Laptop', 'Tower'
        manufacturer VARCHAR(50),       -- 'Dell', 'Lenovo', 'HP'
        model VARCHAR(100),             -- np. 'Optiplex 7050 Micro'
        
        -- Wyciągnięta specyfikacja wewnętrzna
        cpu_model VARCHAR(100),         -- np. 'i5-6500T'
        cpu_passmark INTEGER,           -- Zaciągnięte z bazy offline
        ram_gb INTEGER,
        ram_type VARCHAR(20),           -- 'DDR4 SODIMM'
        storage_gb INTEGER,
        storage_type VARCHAR(50),       -- 'SSD SATA' / 'NVMe'
        
        -- Możliwości klastrowe / rozbudowy
        has_dedicated_gpu BOOLEAN,
        network_interfaces VARCHAR(100),-- Ważne pod Proxmoxa (np. '1x 1Gbps Intel')
        free_ram_slots INTEGER,         -- Czy można dorzucić kostkę
        free_storage_slots INTEGER
    )
""")

print("Tables initialized successfully")

conn.close()
