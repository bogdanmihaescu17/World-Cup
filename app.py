import csv
import io
import logging
import os
import re
from datetime import datetime, timezone
from functools import wraps

from dotenv import load_dotenv
from flask import (
    Flask,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import check_password_hash, generate_password_hash
import openpyxl

load_dotenv()

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-change-me")
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/world_cup",
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = "login"


def configure_logging():
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    app.logger.setLevel(level)
    if not app.logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )
        handler.setFormatter(formatter)
        app.logger.addHandler(handler)


configure_logging()


class User(UserMixin, db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="user")
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    predictions = db.relationship("Prediction", backref="user", lazy=True)

    def set_password(self, raw_password):
        self.password_hash = generate_password_hash(
            raw_password, method="pbkdf2:sha256"
        )

    def check_password(self, raw_password):
        return check_password_hash(self.password_hash, raw_password)


class Match(db.Model):
    __tablename__ = "matches"

    id = db.Column(db.Integer, primary_key=True)
    match_no = db.Column(db.Integer, unique=True, nullable=False)
    group_code = db.Column(db.String(16), nullable=True)
    team1 = db.Column(db.String(120), nullable=False)
    team2 = db.Column(db.String(120), nullable=False)
    kickoff_at = db.Column(db.DateTime, nullable=True)
    venue = db.Column(db.String(120), nullable=True)

    official_score1 = db.Column(db.Integer, nullable=True)
    official_score2 = db.Column(db.Integer, nullable=True)
    official_set_at = db.Column(db.DateTime, nullable=True)

    predictions = db.relationship("Prediction", backref="match", lazy=True)


class Prediction(db.Model):
    __tablename__ = "predictions"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    match_id = db.Column(db.Integer, db.ForeignKey("matches.id"), nullable=False)
    pred_score1 = db.Column(db.Integer, nullable=False)
    pred_score2 = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (db.UniqueConstraint("user_id", "match_id", name="uq_user_match"),)


@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != "admin":
            flash("Admin access required.", "error")
            return redirect(url_for("index"))
        return fn(*args, **kwargs)

    return wrapper


def match_outcome(score1, score2):
    if score1 == score2:
        return "draw"
    return "team1" if score1 > score2 else "team2"


def calculate_points(prediction, match):
    if match.official_score1 is None or match.official_score2 is None:
        return None
    if (
        prediction.pred_score1 == match.official_score1
        and prediction.pred_score2 == match.official_score2
    ):
        return 3
    if match_outcome(prediction.pred_score1, prediction.pred_score2) == match_outcome(
        match.official_score1, match.official_score2
    ):
        return 1
    return 0


def can_edit_prediction(match):
    if not match.kickoff_at:
        return True
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return now < match.kickoff_at


_GROUP_CODE_LETTER = re.compile(r"^([A-L])(\d+)?$", re.IGNORECASE)


def group_letter_from_code(group_code):
    if not group_code:
        return None
    s = str(group_code).strip().upper()
    m = _GROUP_CODE_LETTER.match(s)
    if not m:
        return None
    return m.group(1)


def is_group_stage_match(match):
    return group_letter_from_code(match.group_code) is not None


def group_stage_matches_query():
    return [
        m
        for m in Match.query.order_by(
            Match.kickoff_at.asc().nullslast(), Match.match_no.asc()
        ).all()
        if is_group_stage_match(m)
    ]


def compute_group_standings_from_matches(matches):
    teams = {}
    for m in matches:
        for t in (m.team1, m.team2):
            if t not in teams:
                teams[t] = {
                    "team": t,
                    "pld": 0,
                    "w": 0,
                    "d": 0,
                    "l": 0,
                    "gf": 0,
                    "ga": 0,
                    "pts": 0,
                }
    for m in matches:
        if m.official_score1 is None or m.official_score2 is None:
            continue
        s1, s2 = m.official_score1, m.official_score2
        a, b = m.team1, m.team2
        teams[a]["pld"] += 1
        teams[b]["pld"] += 1
        teams[a]["gf"] += s1
        teams[a]["ga"] += s2
        teams[b]["gf"] += s2
        teams[b]["ga"] += s1
        if s1 > s2:
            teams[a]["w"] += 1
            teams[a]["pts"] += 3
            teams[b]["l"] += 1
        elif s2 > s1:
            teams[b]["w"] += 1
            teams[b]["pts"] += 3
            teams[a]["l"] += 1
        else:
            teams[a]["d"] += 1
            teams[b]["d"] += 1
            teams[a]["pts"] += 1
            teams[b]["pts"] += 1
    rows = []
    for t in teams.values():
        t["gd"] = t["gf"] - t["ga"]
        rows.append(t)
    rows.sort(
        key=lambda r: (-r["pts"], -r["gd"], -r["gf"], r["team"].lower()),
    )
    for idx, r in enumerate(rows, start=1):
        r["rank"] = idx
    return rows


def group_stage_ranking_rows(gs_ids=None):
    if gs_ids is None:
        gs_ids = {m.id for m in group_stage_matches_query()}
    users = User.query.order_by(User.username.asc()).all()
    rows = []
    for user in users:
        score = 0
        for p in user.predictions:
            if p.match_id not in gs_ids:
                continue
            pts = calculate_points(p, p.match)
            if pts is not None:
                score += pts
        rows.append({"username": user.username, "role": user.role, "score": score})
    rows.sort(key=lambda r: (-r["score"], r["username"].lower()))
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    return rows


def ensure_default_admin():
    admin_username = os.getenv("ADMIN_USERNAME", "admin")
    admin_password = os.getenv("ADMIN_PASSWORD", "admin123")
    admin = User.query.filter_by(username=admin_username).first()
    if not admin:
        admin = User(username=admin_username, role="admin")
        admin.set_password(admin_password)
        db.session.add(admin)
        db.session.commit()


def import_excel_data(file_path):
    wb = openpyxl.load_workbook(file_path, data_only=True)
    imported_matches = 0
    imported_users = 0

    if "Matches" in wb.sheetnames:
        ws = wb["Matches"]
        for row in ws.iter_rows(min_row=4, values_only=True):
            match_no = row[1]
            team1 = row[8]
            team2 = row[9]
            if not match_no or not team1 or not team2:
                continue

            kickoff_at = row[5] if isinstance(row[5], datetime) else None
            group_code = str(row[2]).strip() if row[2] else None
            venue = str(row[7]).strip() if row[7] else None

            match = Match.query.filter_by(match_no=int(match_no)).first()
            if not match:
                match = Match(match_no=int(match_no))
                db.session.add(match)
                imported_matches += 1

            match.group_code = group_code
            match.team1 = str(team1).strip()
            match.team2 = str(team2).strip()
            match.kickoff_at = kickoff_at
            match.venue = venue

    for ranking_sheet in ("Predictions_Ranking_1", "Predictions_Ranking_2"):
        if ranking_sheet not in wb.sheetnames:
            continue
        ws = wb[ranking_sheet]
        for row in ws.iter_rows(min_row=4, values_only=True):
            name = row[2] if len(row) > 2 else None
            if not name:
                continue
            username = str(name).strip()
            if not username:
                continue
            if not User.query.filter_by(username=username).first():
                u = User(username=username, role="user")
                u.set_password(os.getenv("DEFAULT_IMPORTED_USER_PASSWORD", "changeme123"))
                db.session.add(u)
                imported_users += 1

    db.session.commit()
    return imported_matches, imported_users


def auto_import_excel_if_empty():
    if os.getenv("AUTO_IMPORT_EXCEL", "true").lower() != "true":
        app.logger.info("Automatic Excel import disabled.")
        return

    if Match.query.count() > 0:
        app.logger.info("Skipping Excel auto-import because matches already exist.")
        return

    path = os.getenv("EXCEL_FILE_PATH", "World Cup_2026.xlsx")
    if not os.path.exists(path):
        app.logger.warning("Excel auto-import skipped; file not found at %s", path)
        return

    imported_matches, imported_users = import_excel_data(path)
    app.logger.info(
        "Excel auto-import completed. New matches=%s, new users=%s",
        imported_matches,
        imported_users,
    )


def ranking_rows():
    users = User.query.order_by(User.username.asc()).all()
    rows = []
    for user in users:
        score = 0
        for p in user.predictions:
            pts = calculate_points(p, p.match)
            if pts is not None:
                score += pts
        rows.append({"username": user.username, "role": user.role, "score": score})

    rows.sort(key=lambda r: (-r["score"], r["username"].lower()))
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    return rows


@app.route("/")
@login_required
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    return {"status": "ok", "service": "world-cup-predictor"}, 200



@app.route("/register", methods=["GET", "POST"])
def register():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        if len(username) < 3 or len(password) < 6:
            flash("Username min 3 chars, password min 6 chars.", "error")
            return redirect(url_for("register"))
        if User.query.filter_by(username=username).first():
            flash("Username already exists.", "error")
            return redirect(url_for("register"))
        user = User(username=username, role="user")
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        flash("Registration successful. Please login.", "success")
        return redirect(url_for("login"))
    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        user = User.query.filter_by(username=username).first()
        if not user or not user.check_password(password):
            flash("Invalid credentials.", "error")
            return redirect(url_for("login"))
        login_user(user)
        return redirect(url_for("index"))
    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


@app.route("/predictions", methods=["GET", "POST"])
@login_required
def predictions():
    if request.method == "POST":
        match_id = int(request.form.get("match_id"))
        score1 = int(request.form.get("pred_score1"))
        score2 = int(request.form.get("pred_score2"))

        match = Match.query.get_or_404(match_id)
        if not can_edit_prediction(match):
            flash("Prediction is locked after kickoff.", "error")
            return redirect(url_for("predictions"))

        pred = Prediction.query.filter_by(user_id=current_user.id, match_id=match.id).first()
        if not pred:
            pred = Prediction(user_id=current_user.id, match_id=match.id, pred_score1=score1, pred_score2=score2)
            db.session.add(pred)
        else:
            pred.pred_score1 = score1
            pred.pred_score2 = score2
        db.session.commit()
        flash("Prediction saved.", "success")
        return redirect(url_for("predictions"))

    matches = Match.query.order_by(Match.kickoff_at.asc().nullslast(), Match.match_no.asc()).all()
    predictions_map = {
        p.match_id: p
        for p in Prediction.query.filter_by(user_id=current_user.id).all()
    }
    return render_template(
        "predictions.html",
        matches=matches,
        predictions_map=predictions_map,
        calculate_points=calculate_points,
        can_edit_prediction=can_edit_prediction,
    )


@app.route("/group-stage", methods=["GET", "POST"])
@login_required
def group_stage():
    if request.method == "POST":
        match_id = int(request.form.get("match_id"))
        score1 = int(request.form.get("pred_score1"))
        score2 = int(request.form.get("pred_score2"))

        match = Match.query.get_or_404(match_id)
        if not is_group_stage_match(match):
            flash("This match is not part of the group stage.", "error")
            return redirect(url_for("group_stage"))
        if not can_edit_prediction(match):
            flash("Prediction is locked after kickoff.", "error")
            return redirect(url_for("group_stage"))

        pred = Prediction.query.filter_by(
            user_id=current_user.id, match_id=match.id
        ).first()
        if not pred:
            pred = Prediction(
                user_id=current_user.id,
                match_id=match.id,
                pred_score1=score1,
                pred_score2=score2,
            )
            db.session.add(pred)
        else:
            pred.pred_score1 = score1
            pred.pred_score2 = score2
        db.session.commit()
        flash("Prediction saved.", "success")
        return redirect(url_for("group_stage"))

    all_gs = group_stage_matches_query()
    gs_ids = {m.id for m in all_gs}
    letters = sorted(
        {group_letter_from_code(m.group_code) for m in all_gs},
    )
    groups_data = []
    for letter in letters:
        g_matches = [
            m for m in all_gs if group_letter_from_code(m.group_code) == letter
        ]
        groups_data.append(
            {
                "letter": letter,
                "matches": g_matches,
                "standings": compute_group_standings_from_matches(g_matches),
            }
        )

    predictions_map = {
        p.match_id: p
        for p in Prediction.query.filter_by(user_id=current_user.id).all()
    }
    gs_ranking = group_stage_ranking_rows(gs_ids)
    return render_template(
        "group_stage.html",
        groups_data=groups_data,
        predictions_map=predictions_map,
        calculate_points=calculate_points,
        can_edit_prediction=can_edit_prediction,
        group_stage_ranking_rows=gs_ranking,
    )


@app.route("/ranking")
@login_required
def ranking():
    return render_template("ranking.html", rows=ranking_rows())


@app.route("/admin")
@login_required
@admin_required
def admin_panel():
    matches = Match.query.order_by(Match.kickoff_at.asc().nullslast(), Match.match_no.asc()).all()
    users = User.query.order_by(User.username.asc()).all()
    return render_template("admin.html", matches=matches, users=users)


@app.route("/admin/users", methods=["POST"])
@login_required
@admin_required
def admin_create_user():
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    role = request.form.get("role", "user").strip()
    if role not in ("admin", "user"):
        role = "user"
    if not username or len(password) < 6:
        flash("Username required, password min 6 chars.", "error")
        return redirect(url_for("admin_panel"))
    if User.query.filter_by(username=username).first():
        flash("Username already exists.", "error")
        return redirect(url_for("admin_panel"))
    user = User(username=username, role=role)
    user.set_password(password)
    db.session.add(user)
    db.session.commit()
    flash("User created.", "success")
    return redirect(url_for("admin_panel"))


@app.route("/admin/users/<int:user_id>/role", methods=["POST"])
@login_required
@admin_required
def admin_update_user_role(user_id):
    user = User.query.get_or_404(user_id)
    role = request.form.get("role", "user")
    if role not in ("admin", "user"):
        role = "user"
    user.role = role
    db.session.commit()
    flash("User role updated.", "success")
    return redirect(url_for("admin_panel"))


@app.route("/admin/users/<int:user_id>/delete", methods=["POST"])
@login_required
@admin_required
def admin_delete_user(user_id):
    user = User.query.get_or_404(user_id)
    if user.id == current_user.id:
        flash("You cannot delete your own admin account.", "error")
        return redirect(url_for("admin_panel"))
    Prediction.query.filter_by(user_id=user.id).delete()
    db.session.delete(user)
    db.session.commit()
    flash("User deleted.", "success")
    return redirect(url_for("admin_panel"))


@app.route("/admin/match-score", methods=["POST"])
@login_required
@admin_required
def admin_set_official_score():
    match_id = int(request.form.get("match_id"))
    score1 = int(request.form.get("official_score1"))
    score2 = int(request.form.get("official_score2"))
    match = Match.query.get_or_404(match_id)
    match.official_score1 = score1
    match.official_score2 = score2
    match.official_set_at = datetime.utcnow()
    db.session.commit()
    flash("Official score saved. Ranking is now updated.", "success")
    return redirect(url_for("admin_panel"))


@app.route("/admin/import-excel", methods=["POST"])
@login_required
@admin_required
def admin_import_excel():
    path = os.getenv("EXCEL_FILE_PATH", "World Cup_2026.xlsx")
    if not os.path.exists(path):
        flash(f"Excel file not found at: {path}", "error")
        return redirect(url_for("admin_panel"))
    matches, users = import_excel_data(path)
    flash(f"Excel imported. New matches: {matches}, new users: {users}.", "success")
    return redirect(url_for("admin_panel"))


@app.route("/admin/ranking-export")
@login_required
@admin_required
def admin_export_ranking():
    rows = ranking_rows()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["rank", "username", "role", "score"])
    for row in rows:
        writer.writerow([row["rank"], row["username"], row["role"], row["score"]])
    mem = io.BytesIO()
    mem.write(output.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(
        mem,
        as_attachment=True,
        download_name="ranking.csv",
        mimetype="text/csv",
    )


def init_database():
    with app.app_context():
        db.create_all()
        ensure_default_admin()
        auto_import_excel_if_empty()


if os.getenv("AUTO_INIT_DB", "true").lower() == "true":
    init_database()
    app.logger.info("Database auto-init completed.")
else:
    app.logger.info("Database auto-init skipped.")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001"))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.logger.info("Starting app on port %s (debug=%s)", port, debug)
    app.run(host="0.0.0.0", port=port, debug=debug)
