import json, random, argparse
from datetime import date, timedelta
from faker import Faker

fake = Faker()

def load_profiles(path):
    return [json.loads(line) for line in open(path)]

def main(profiles, out, n):
    data = []
    start = date(2025,6,1)
    for _ in range(n):
        p = random.choice(profiles)
        rec = {
            "pet_id": p["pet_id"],
            "date": (start + timedelta(days=random.randint(0,29))).isoformat(),
            "food_amount": random.choice([30,45,60,75]),
            "water_intake": random.randint(40,120),
            "sleep_hours": round(random.uniform(10,16),1),
            "movement_count": random.randint(50,400),
            "meow_count": random.randint(0,15),
            "weight_kg": round(random.uniform(3,6),1)
        }
        data.append(rec)
    with open(out,"w") as f:
        for r in data:
            f.write(json.dumps(r)+"\n")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--profiles", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--records", type=int, default=10000)
    args=ap.parse_args()
    profiles = load_profiles(args.profiles)
    main(profiles, args.output, args.records)