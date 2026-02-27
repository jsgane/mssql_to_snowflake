#!/usr/bin/env python3
"""
Test BCP direct pour voir l'erreur réelle
"""

import subprocess
import os

# Config
server = "BODSQLDEV\\DATAWH"
database = "NMBMTS"
user = "sqltalend"
password = "sql1234*"
table = "a_bronze_mns_d_site"

# Commande BCP
cmd = [
    "bcp",
    f"SELECT TOP 10 * FROM {table}",
    "queryout",
    "/tmp/test_bcp.csv",
    "-c",
    "-t", "|",
    "-S", server,
    "-d", database,
    "-U", user,
    "-P", password,
]

print("🔄 Test BCP...")
print(f"   Serveur: {server}")
print(f"   Database: {database}")
print(f"   Table: {table}")
print()

# Exécuter
result = subprocess.run(cmd, capture_output=True, text=True)

print(f"Code retour: {result.returncode}")
print()

if result.stdout:
    print("=== STDOUT ===")
    print(result.stdout)
    print()

if result.stderr:
    print("=== STDERR ===")
    print(result.stderr)
    print()

if result.returncode == 0:
    print("✅ BCP fonctionne!")
    
    # Voir le contenu
    with open("/tmp/test_bcp.csv") as f:
        lines = f.readlines()
        print(f"Lignes extraites: {len(lines)}")
        if lines:
            print(f"Première ligne: {lines[0][:100]}")
else:
    print("❌ BCP a échoué")
    print()
    print("🔍 Suggestions de debug:")
    print(f"   1. Test connexion: telnet {server.split(chr(92))[0]} 1433")
    print(f"   2. Test SQL: sqlcmd -S {server} -d {database} -U {user} -P {password} -Q 'SELECT @@VERSION'")
    print(f"   3. Vérifier table: sqlcmd -S {server} -d {database} -U {user} -P {password} -Q 'SELECT TOP 1 * FROM {table}'")