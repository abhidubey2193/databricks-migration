test:      ; python -m unittest discover -s tests -t . -v
scan-demo: ; PYTHONPATH=src python -m smc.cli scan --fixtures tests/fixtures/fleet.json --mount-map '{"/mnt/raw":"/Volumes/main/lake/raw"}'
coverage:  ; PYTHONPATH=src python -m smc.cli coverage
fixtures:  ; python demo/build_fixtures.py
