<div align="center">
 <h1>cijenikAB</h1>
<img src="static/icon.png" alt="cijenikAB logo" width="256" />
<h1></h1>
</div>

Uoči novog zakona [NN 75/2025 (2.5.2025.) (Odluka o objavi cjenika i isticanju dodatne cijene kao mjera izravne kontrole cijena u trgovini na malo)](https://www.zakon.hr/c/podzakonski-propis/540799/nn-75-2025-%282.5.2025.%29%2C-odluka-o-objavi-cjenika-i-isticanju-dodatne-cijene-kao-mjera-izravne-kontrole-cijena-u-trgovini-na), većina ako ne sve maloprodajne trgovine izdavaju 
> cjenike u digitalnom obliku, pogodne za automatsku obradu

Ovaj alat pokušava to što jednostavnije obaviti.

*Napravljeno po ideji https://usporedicijene.info*

## Korištenje
Pristupiti se može preko stranice https://cijene.brinu.me

Također, možete `git clone`-ati repository, napraviti venv i pokrenuti `pip install -r requirements.txt` njime te `uvicorn app:app` u tom direktoriju.

## Značajke
1. [x] Pretraga po imenu ili barkodu
2. [x] Sortiranje po trgovini, kategoriji i adresi
3. [ ] Statistika
4. [ ] Košarica
5. [ ] Skeniranje kodova

## Podržane trgovine
- Spar
- Konzum
- Ribola
- Studenac
- Plodine

