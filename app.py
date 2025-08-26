from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
import psycopg2
import os
from datetime import datetime, timedelta
import hashlib
from geopy.distance import geodesic
from io import StringIO
import csv
from flask import Response

app = Flask(__name__)
app.secret_key = 'your_secret_key_change_this_in_production'

############################### connection #################################

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "dpg-d2modeur433s73auu6qg-a"),
        database=os.getenv("DB_NAME", "army_db"),
        user=os.getenv("DB_USER", "army_db_user"),
        password=os.getenv("DB_PASSWORD", "oCfPpl0fp9ycRVHx40l71YLKGMsjoZWZ"),
        port=os.getenv("DB_PORT", "5432"),
        sslmode=os.getenv("DB_SSLMODE", "require"),
    )
    return conn

################################# start ##########################################

@app.route('/')
def index():
    # If user is already logged in, redirect to dashboard
    if session.get('logged_in'):
        return redirect(url_for('dashboard'))
    return render_template('login.html')

################################ login page #####################################

@app.route('/login', methods=['POST'])
def login():
    username = request.form.get('username')
    password = request.form.get('password')

    if not username or not password:
        flash('All fields are required', 'error')
        return redirect('/')

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query to check if username and password exist
        cursor.execute("SELECT id, full_name, username, password_hash FROM admins WHERE username = %s", (username,))
        user = cursor.fetchone()

        if user:
            # Store user data in variables before closing connection
            user_id = user[0]
            full_name = user[1]
            user_username = user[2]
            stored_password = user[3]

            # Close connection before password verification
            cursor.close()
            conn.close()

            # Simple password verification (replace with proper hash verification in production)
            if password == stored_password or hashlib.sha256(password.encode()).hexdigest() == stored_password:
                # Set session variables
                session['logged_in'] = True
                session['user_id'] = user_id
                session['username'] = user_username
                session['full_name'] = full_name

                flash('Login successful! Welcome to the dashboard.', 'success')
                return redirect(url_for('dashboard'))
            else:
                flash('Invalid username or password. Please try again.', 'error')
        else:
            cursor.close()
            conn.close()
            flash('Invalid username or password. Please try again.', 'error')

    except Exception as e:
        flash('Database connection error. Please try again later.', 'error')
        print(f"Database error: {e}")
        # Clean up connections in case of error
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return redirect('/')

################################# dashboard ##################################

@app.route('/dashboard')
def dashboard():
    # Check if user is logged in
    if not session.get('logged_in'):
        flash('Please login to access the dashboard.', 'error')
        return redirect('/')

    # Check maintenance notifications when dashboard loads
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # First, delete all notifications with status = 'read'
        cursor.execute("DELETE FROM notifications WHERE status = 'read'")

        current_date = datetime.now().date()

        # Get all maintenance records with status 'not'
        cursor.execute("""
            SELECT mr.id, mr.vehicle_id, mr.type, mr.next_due, mr.next_due_mileage, 
                   mr.periodicity, v.BA_number, v.total_milage
            FROM maintenance_records mr
            JOIN vehicles v ON mr.vehicle_id = v.id
            WHERE mr.status = 'not'
        """)

        maintenance_records = cursor.fetchall()

        for record in maintenance_records:
            record_id, vehicle_id, maintenance_type, next_due, next_due_mileage, periodicity, ba_number, total_mileage = record

            should_notify = False
            message = ""

            # Check based on periodicity
            if periodicity in [0, 2]:  # Time-based or both
                if next_due:
                    days_diff = (next_due - current_date).days
                    if days_diff <= 7:
                        should_notify = True
                        if days_diff < 0:
                            message = f"{maintenance_type} maintenance of vehicle {ba_number} is overdue and was scheduled on {next_due.strftime('%Y-%m-%d')}"
                        else:
                            message = f"{maintenance_type} maintenance of vehicle {ba_number} is scheduled this week on {next_due.strftime('%Y-%m-%d')}"

            if periodicity in [1, 2]:  # Distance-based or both
                if next_due_mileage and total_mileage:
                    mileage_diff = next_due_mileage - total_mileage
                    if mileage_diff <= 5:
                        should_notify = True
                        if mileage_diff < 0:
                            message = f"{maintenance_type} maintenance of vehicle {ba_number} is overdue and was scheduled at {next_due_mileage} km mileage"
                        else:
                            message = f"{maintenance_type} maintenance of vehicle {ba_number} is due soon at {next_due_mileage} km mileage"

            if should_notify:
                # Check if notification already exists for this type and vehicle
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM notifications 
                    WHERE type = %s AND vehicle_id = %s
                """, (maintenance_type, vehicle_id))

                existing_count = cursor.fetchone()[0]

                if existing_count == 0:
                    # Insert new notification
                    cursor.execute("""
                        INSERT INTO notifications (type, vehicle_id, message, timestamp, status)
                        VALUES (%s, %s, %s, %s, 'unread')
                    """, (maintenance_type, vehicle_id, message, datetime.now()))

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error checking maintenance notifications: {e}")
        # Don't let notification errors break the dashboard
        pass

    # Get user information from session
    user_info = {
        'full_name': session.get('full_name'),
        'username': session.get('username'),
        'user_id': session.get('user_id')
    }

    return render_template('dashboard.html', user=user_info)

################################# logout ##################################

@app.route('/logout')
def logout():
    # Clear all session data
    session.clear()
    flash('You have been successfully logged out.', 'success')
    return redirect('/')

################################# additional dashboard route ##############

@app.route('/check_session')
def check_session():
    """Route to check if user session is still valid"""
    if session.get('logged_in'):
        return {'status': 'valid', 'username': session.get('username')}
    else:
        return {'status': 'invalid'}

# Pages redirection Routes

@app.route('/drivers.html')
def manage_drivers():
    if not session.get('logged_in'):
        flash('Unauthorized access. Please login.')
        return render_template('login.html')
    return render_template('drivers.html')

@app.route('/vehicles.html')
def manage_vehicles():
    if not session.get('logged_in'):
        flash('Unauthorized access. Please login.')
        return render_template('login.html')
    return render_template('vehicles.html')

@app.route('/maintenance.html')
def manage_maintenance():
    if not session.get('logged_in'):
        flash('Unauthorized access. Please login.')
        return render_template('login.html')
    return render_template('maintenance.html')

@app.route('/live_tracking.html')
def live_tracking():
    if not session.get('logged_in'):
        flash('Unauthorized access. Please login.')
        return render_template('login.html')
    return render_template('live_tracking.html')

@app.route('/route_tracking.html')
def route_tracking():
    if not session.get('logged_in'):
        flash('Unauthorized access. Please login.')
        return render_template('login.html')
    return render_template('route_tracking.html')

@app.route('/admins.html')
def manage_admins():
    if not session.get('logged_in'):
        flash('Unauthorized access. Please login.')
        return render_template('login.html')
    return render_template('admins.html')

@app.route('/notifications.html')
def view_notifications():
    if not session.get('logged_in'):
        flash('Unauthorized access. Please login.')
        return render_template('login.html')
    return render_template('notifications.html')

# notification count

@app.route('/get_notification_count')
def get_notification_count():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Count unread notifications
        cursor.execute("""
                       SELECT COUNT(*) as count
                       FROM notifications
                       WHERE status = 'unread'
                       """)
        result = cursor.fetchone()
        count = result.count if result else 0

        cursor.close()
        conn.close()

        return jsonify({'count': count})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/check_maintenance_notifications')
def check_maintenance_notifications():
    """Check for maintenance due soon or overdue and create notifications"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        current_date = datetime.now().date()
        notifications_created = 0

        # Get all maintenance records with status 'not'
        cursor.execute("""
            SELECT mr.id, mr.vehicle_id, mr.type, mr.next_due, mr.next_due_mileage, 
                   mr.periodicity, v.BA_number, v.total_milage
            FROM maintenance_records mr
            JOIN vehicles v ON mr.vehicle_id = v.id
            WHERE mr.status = 'not'
        """)

        maintenance_records = cursor.fetchall()

        for record in maintenance_records:
            record_id, vehicle_id, maintenance_type, next_due, next_due_mileage, periodicity, ba_number, total_mileage = record

            should_notify = False
            message = ""
            is_overdue = False

            # Check based on periodicity
            if periodicity in [0, 2]:  # Time-based or both
                if next_due:
                    days_diff = (next_due - current_date).days
                    if days_diff <= 7:
                        should_notify = True
                        if days_diff < 0:
                            is_overdue = True
                            message = f"{maintenance_type} maintenance of vehicle {ba_number} is overdue and was scheduled on {next_due.strftime('%Y-%m-%d')}"
                        else:
                            message = f"{maintenance_type} maintenance of vehicle {ba_number} is scheduled this week on {next_due.strftime('%Y-%m-%d')}"

            if periodicity in [1, 2]:  # Distance-based or both
                if next_due_mileage and total_mileage:
                    mileage_diff = next_due_mileage - total_mileage
                    if mileage_diff <= 5:
                        should_notify = True
                        if mileage_diff < 0:
                            is_overdue = True
                            message = f"{maintenance_type} maintenance of vehicle {ba_number} is overdue and was scheduled at {next_due_mileage} km mileage"
                        else:
                            message = f"{maintenance_type} maintenance of vehicle {ba_number} is due soon at {next_due_mileage} km mileage"

            if should_notify:
                # Check if notification already exists for this type and vehicle
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM notifications 
                    WHERE type = %s AND vehicle_id = %s
                """, (maintenance_type, vehicle_id))

                existing_count = cursor.fetchone()[0]

                if existing_count == 0:
                    # Insert new notification
                    cursor.execute("""
                        INSERT INTO notifications (type, vehicle_id, message, timestamp, status)
                        VALUES (%s, %s, %s, %s, 'unread')
                    """, (maintenance_type, vehicle_id, message, datetime.now()))

                    notifications_created += 1

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'notifications_created': notifications_created,
            'message': f'Maintenance check completed. {notifications_created} new notifications created.'
        })

    except Exception as e:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500

###################################### notifications page ##################################

@app.route('/get_notifications')
def get_notifications():
    """Get all notifications from database"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, type, vehicle_id, message, timestamp, status
            FROM notifications
            ORDER BY timestamp DESC
        """)

        notifications = []
        for row in cursor.fetchall():
            notifications.append({
                'id': row[0],
                'type': row[1],
                'vehicle_id': row[2],
                'message': row[3],
                'timestamp': row[4].strftime('%Y-%m-%d %H:%M:%S') if row[4] else None,
                'status': row[5]
            })

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'notifications': notifications
        })

    except Exception as e:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500

@app.route('/mark_all_notifications_read', methods=['POST'])
def mark_all_notifications_read():
    """Mark all notifications as read"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get count of unread notifications
        cursor.execute("SELECT COUNT(*) FROM notifications WHERE status = 'unread'")
        unread_count = cursor.fetchone()[0]

        # Mark all as read
        cursor.execute("UPDATE notifications SET status = 'read'")

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'updated_count': unread_count,
            'message': f'Successfully marked {unread_count} notifications as read.'
        })

    except Exception as e:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500

@app.route('/mark_notification_read', methods=['POST'])
def mark_notification_read():
    """Mark a single notification as read"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = None
    cursor = None
    try:
        data = request.get_json()
        notification_id = data.get('notification_id')

        if not notification_id:
            return jsonify({'error': 'Notification ID is required'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE notifications 
            SET status = 'read' 
            WHERE id = %s
        """, (notification_id,))

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Notification not found'}), 404

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'Notification marked as read successfully.'
        })

    except Exception as e:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500

@app.route('/delete_all_notifications', methods=['POST'])
def delete_all_notifications():
    """Delete all notifications from database"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get count of notifications to delete
        cursor.execute("SELECT COUNT(*) FROM notifications")
        total_count = cursor.fetchone()[0]

        # Delete all notifications
        cursor.execute("DELETE FROM notifications")

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'deleted_count': total_count,
            'message': f'Successfully deleted {total_count} notifications.'
        })

    except Exception as e:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500

@app.route('/delete_notification', methods=['POST'])
def delete_notification():
    """Delete a single notification from database"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        data = request.get_json()
        notification_id = data.get('notification_id')

        if not notification_id:
            return jsonify({'error': 'Notification ID is required'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM notifications WHERE id = %s", (notification_id,))

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Notification not found'}), 404

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'Notification deleted successfully.'
        })

    except Exception as e:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500

@app.route('/check_maintenance_notifications', methods=['POST'])
def check_maintenance_notifications_post():
    """POST version of check maintenance notifications for the refresh button"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        current_date = datetime.now().date()
        notifications_created = 0

        # Get all maintenance records with status 'not'
        cursor.execute("""
            SELECT mr.id, mr.vehicle_id, mr.type, mr.next_due, mr.next_due_mileage, 
                   mr.periodicity, v.BA_number, v.total_milage
            FROM maintenance_records mr
            JOIN vehicles v ON mr.vehicle_id = v.id
            WHERE mr.status = 'not'
        """)

        maintenance_records = cursor.fetchall()

        for record in maintenance_records:
            record_id, vehicle_id, maintenance_type, next_due, next_due_mileage, periodicity, ba_number, total_mileage = record

            should_notify = False
            message = ""
            is_overdue = False

            # Check based on periodicity
            if periodicity in [0, 2]:  # Time-based or both
                if next_due:
                    days_diff = (next_due - current_date).days
                    if days_diff <= 7:
                        should_notify = True
                        if days_diff < 0:
                            is_overdue = True
                            message = f"{maintenance_type} maintenance of vehicle {ba_number} is overdue and was scheduled on {next_due.strftime('%Y-%m-%d')}"
                        else:
                            message = f"{maintenance_type} maintenance of vehicle {ba_number} is scheduled this week on {next_due.strftime('%Y-%m-%d')}"

            if periodicity in [1, 2]:  # Distance-based or both
                if next_due_mileage and total_mileage:
                    mileage_diff = next_due_mileage - total_mileage
                    if mileage_diff <= 5:
                        should_notify = True
                        if mileage_diff < 0:
                            is_overdue = True
                            message = f"{maintenance_type} maintenance of vehicle {ba_number} is overdue and was scheduled at {next_due_mileage} km mileage"
                        else:
                            message = f"{maintenance_type} maintenance of vehicle {ba_number} is due soon at {next_due_mileage} km mileage"

            if should_notify:
                # Check if notification already exists for this type and vehicle
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM notifications 
                    WHERE type = %s AND vehicle_id = %s
                """, (maintenance_type, vehicle_id))

                existing_count = cursor.fetchone()[0]

                if existing_count == 0:
                    # Insert new notification
                    cursor.execute("""
                        INSERT INTO notifications (type, vehicle_id, message, timestamp, status)
                        VALUES (%s, %s, %s, %s, 'unread')
                    """, (maintenance_type, vehicle_id, message, datetime.now()))

                    notifications_created += 1

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'notifications_created': notifications_created,
            'message': f'Maintenance check completed. {notifications_created} new notifications created.'
        })

    except Exception as e:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500

######################################## driver page route #############################

@app.route('/add_driver', methods=['POST'])
def add_driver():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()

        driver_id = data.get('id', '').strip()
        name = data.get('name', '').strip()
        rank = data.get('rank', '').strip() or None
        army_number = data.get('army_number', '').strip()
        unit = data.get('unit', '').strip() or None

        if not driver_id or not name or not army_number:
            return jsonify({'success': False, 'message': 'Driver ID, Name, and Army Number are required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if driver ID already exists
        cursor.execute("SELECT id FROM drivers WHERE id = %s", (driver_id,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Driver ID already exists'})

        # Check if army number already exists
        cursor.execute("SELECT army_number FROM drivers WHERE army_number = %s", (army_number,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Army number already exists'})

        # Insert new driver
        cursor.execute("""
            INSERT INTO drivers (id, name, rank, army_number, unit)
            VALUES (%s, %s, %s, %s, %s)
        """, (driver_id, name, rank, army_number, unit))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'message': 'Driver added successfully'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/get_drivers')
def get_drivers():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        page = int(request.args.get('page', 1))
        per_page = 10
        search_name = request.args.get('search_name', '').strip()
        unit_filter = request.args.get('unit', '').strip()

        conn = get_db_connection()
        cursor = conn.cursor()

        # Build WHERE clause
        where_conditions = []
        params = []

        if search_name:
            where_conditions.append("name LIKE %s")
            params.append(f"%{search_name}%")

        if unit_filter:
            where_conditions.append("unit = %s")
            params.append(unit_filter)

        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)

        # Get total count
        count_query = f"SELECT COUNT(*) FROM drivers {where_clause}"
        cursor.execute(count_query, params)
        total_records = cursor.fetchone()[0]
        total_pages = (total_records + per_page - 1) // per_page

        # Get drivers with pagination
        offset = (page - 1) * per_page
        query = f"""
            SELECT id, name, rank, army_number, unit
            FROM drivers {where_clause}
            ORDER BY name
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """
        cursor.execute(query, params + [offset, per_page])

        drivers = []
        for row in cursor.fetchall():
            drivers.append({
                'id': row[0],
                'name': row[1],
                'rank': row[2],
                'army_number': row[3],
                'unit': row[4]
            })

        cursor.close()
        conn.close()

        return jsonify({
            'drivers': drivers,
            'total_pages': total_pages,
            'current_page': page,
            'total_records': total_records
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_units')
def get_units():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT unit FROM drivers WHERE unit IS NOT NULL AND unit != '' ORDER BY unit")
        units = [row[0] for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        return jsonify({'units': units})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_driver_by_id')
def get_driver_by_id():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        driver_id = request.args.get('driver_id', '').strip()

        if not driver_id:
            return jsonify({'success': False, 'message': 'Driver ID is required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, name, rank, army_number, unit FROM drivers WHERE id = %s", (driver_id,))
        row = cursor.fetchone()

        if row:
            driver = {
                'id': row[0],
                'name': row[1],
                'rank': row[2],
                'army_number': row[3],
                'unit': row[4]
            }
            cursor.close()
            conn.close()
            return jsonify({'driver': driver})
        else:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Driver not found'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/get_driver_by_army_number')
def get_driver_by_army_number():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        army_number = request.args.get('army_number', '').strip()

        if not army_number:
            return jsonify({'success': False, 'message': 'Army number is required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, name, rank, army_number, unit FROM drivers WHERE army_number = %s", (army_number,))
        row = cursor.fetchone()

        if row:
            driver = {
                'id': row[0],
                'name': row[1],
                'rank': row[2],
                'army_number': row[3],
                'unit': row[4]
            }
            cursor.close()
            conn.close()
            return jsonify({'driver': driver})
        else:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Driver not found'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/update_driver', methods=['POST'])
def update_driver():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()

        driver_id = data.get('driver_id', '').strip()
        new_driver_id = data.get('new_driver_id', '').strip()
        name = data.get('name', '').strip()
        rank = data.get('rank', '').strip() or None
        army_number = data.get('army_number', '').strip()
        unit = data.get('unit', '').strip() or None

        if not driver_id or not new_driver_id or not name or not army_number:
            return jsonify({'success': False, 'message': 'All required fields must be filled'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if new driver ID already exists (if different from current)
        if driver_id != new_driver_id:
            cursor.execute("SELECT id FROM drivers WHERE id = %s", (new_driver_id,))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'message': 'New Driver ID already exists'})

        # Check if army number already exists (if different from current driver's army number)
        cursor.execute("SELECT army_number, id FROM drivers WHERE army_number = %s", (army_number,))
        existing = cursor.fetchone()
        if existing and existing[1] != driver_id:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Army number already exists for another driver'})

        # Update driver
        cursor.execute("""
            UPDATE drivers 
            SET id = %s, name = %s, rank = %s, army_number = %s, unit = %s
            WHERE id = %s
        """, (new_driver_id, name, rank, army_number, unit, driver_id))

        # If driver ID changed, update related tables
        if driver_id != new_driver_id:
            cursor.execute("UPDATE GPSData SET driver_id = %s WHERE driver_id = %s", (new_driver_id, driver_id))
            cursor.execute("UPDATE events SET driver_id = %s WHERE driver_id = %s", (new_driver_id, driver_id))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'message': 'Driver updated successfully'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/remove_driver', methods=['POST'])
def remove_driver():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        driver_id = data.get('driver_id', '').strip()

        if not driver_id:
            return jsonify({'success': False, 'message': 'Driver ID is required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if driver exists
        cursor.execute("SELECT id FROM drivers WHERE id = %s", (driver_id,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Driver not found'})

        # Delete related data from other tables
        cursor.execute("DELETE FROM events WHERE driver_id = %s", (driver_id,))
        cursor.execute("DELETE FROM GPSData WHERE driver_id = %s", (driver_id,))

        # Delete driver
        cursor.execute("DELETE FROM drivers WHERE id = %s", (driver_id,))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'message': 'Driver and all related data removed successfully'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/generate_driver_report', methods=['POST'])
def generate_driver_report():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        driver_id = data.get('driver_id', '').strip()
        start_date = data.get('start_date', '').strip()
        end_date = data.get('end_date', '').strip()

        if not driver_id or not start_date or not end_date:
            return jsonify({'success': False, 'message': 'Driver ID, start date, and end date are required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Get driver information
        cursor.execute("SELECT id, name, rank, army_number, unit FROM drivers WHERE id = %s", (driver_id,))
        driver_row = cursor.fetchone()

        if not driver_row:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Driver not found'})

        driver_info = {
            'id': driver_row[0],
            'name': driver_row[1],
            'rank': driver_row[2],
            'army_number': driver_row[3],
            'unit': driver_row[4]
        }

        # Get vehicles driven by the driver in the specified period
        cursor.execute("""
            SELECT DISTINCT v.id, v.make, v.model, v.BA_number
            FROM vehicles v
            INNER JOIN GPSData g ON v.id = g.vehicle_id
            WHERE g.driver_id = %s AND CAST(g.timestamp AS DATE) BETWEEN %s AND %s
        """, (driver_id, start_date, end_date))

        vehicles = []
        total_distance = 0

        for vehicle_row in cursor.fetchall():
            vehicle_id = vehicle_row[0]
            vehicle_make = vehicle_row[1] or 'Unknown Make'
            vehicle_model = vehicle_row[2] or 'Unknown Model'
            ba_number = vehicle_row[3] or 'Unknown BA Number'

            # Calculate distance for this vehicle
            cursor.execute("""
                SELECT lat, lon FROM GPSData 
                WHERE vehicle_id = %s AND driver_id = %s 
                AND CAST(timestamp AS DATE) BETWEEN %s AND %s
                ORDER BY timestamp
            """, (vehicle_id, driver_id, start_date, end_date))

            gps_points = cursor.fetchall()
            vehicle_distance = 0

            # Distance calculation using geopy
            for i in range(1, len(gps_points)):
                prev_lat, prev_lon = gps_points[i - 1]
                curr_lat, curr_lon = gps_points[i]
                distance = geodesic((prev_lat, prev_lon), (curr_lat, curr_lon)).kilometers
                vehicle_distance += distance

            # Get events for this vehicle and driver
            cursor.execute("""
                SELECT event_type, COUNT(*) as count
                FROM events 
                WHERE vehicle_id = %s AND driver_id = %s 
                AND CAST(timestamp AS DATE) BETWEEN %s AND %s
                AND event_type IN ('harsh_brake', 'overspeeding', 'harsh_acceleration')
                GROUP BY event_type
            """, (vehicle_id, driver_id, start_date, end_date))

            events = {
                'harsh_brake': 0,
                'overspeeding': 0,
                'harsh_acceleration': 0
            }

            for event_row in cursor.fetchall():
                events[event_row[0]] = event_row[1]

            vehicles.append({
                'VehicleName': f"{vehicle_make} {vehicle_model}",
                'BA_number': ba_number,
                'distance': round(vehicle_distance, 2),
                'events': events
            })

            total_distance += vehicle_distance

        cursor.close()
        conn.close()

        report_data = {
            'driver': driver_info,
            'start_date': start_date,
            'end_date': end_date,
            'total_distance': round(total_distance, 2),
            'vehicles': vehicles
        }

        return jsonify({'success': True, 'report': report_data})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/export_events_csv', methods=['POST'])
def export_events_csv():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        driver_id = data.get('driver_id', '').strip()
        start_date = data.get('start_date', '').strip()
        end_date = data.get('end_date', '').strip()
        event_type = data.get('event_type', '').strip()

        if not driver_id or not start_date or not end_date or not event_type:
            return jsonify({'success': False, 'message': 'Driver ID, start date, end date, and event type are required'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        # Get driver information
        cursor.execute("SELECT id, name, rank, army_number, unit FROM drivers WHERE id = %s", (driver_id,))
        driver_row = cursor.fetchone()
        if not driver_row:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Driver not found'}), 404

        driver_info = {
            'id': driver_row[0],
            'name': driver_row[1],
            'rank': driver_row[2] or 'N/A',
            'army_number': driver_row[3],
            'unit': driver_row[4] or 'N/A'
        }

        # Prepare query for events
        query = """
            SELECT 
                e.id, 
                e.vehicle_id, 
                v.BA_number, 
                v.make, 
                v.model, 
                v.type, 
                v.unit AS vehicle_unit, 
                v.maintaining_workshop, 
                e.timestamp, 
                e.lat, 
                e.lon, 
                e.event_type
            FROM events e
            INNER JOIN vehicles v ON e.vehicle_id = v.id
            WHERE e.driver_id = %s 
            AND CAST(e.timestamp AS DATE) BETWEEN %s AND %s
        """
        params = [driver_id, start_date, end_date]

        if event_type != 'all':
            query += " AND e.event_type = %s"
            params.append(event_type)

        query += " ORDER BY e.timestamp"

        cursor.execute(query, params)
        events = cursor.fetchall()

        # Create CSV in memory
        output = StringIO()
        writer = csv.writer(output, lineterminator='\n')

        # Write header
        headers = [
            'Event ID',
            'Driver ID',
            'Driver Name',
            'Driver Rank',
            'Driver Army Number',
            'Driver Unit',
            'Vehicle ID',
            'Vehicle BA Number',
            'Vehicle Make',
            'Vehicle Model',
            'Vehicle Type',
            'Vehicle Unit',
            'Vehicle Maintaining Workshop',
            'Timestamp',
            'Latitude',
            'Longitude',
            'Event Type'
        ]
        writer.writerow(headers)

        # Write data rows
        for event in events:
            writer.writerow([
                event[0],  # Event ID
                driver_info['id'],
                driver_info['name'],
                driver_info['rank'],
                driver_info['army_number'],
                driver_info['unit'],
                event[1],  # Vehicle ID
                event[2],  # BA Number
                event[3],  # Make
                event[4],  # Model
                event[5],  # Type
                event[6],  # Vehicle Unit
                event[7],  # Maintaining Workshop
                event[8],  # Timestamp
                event[9],  # Latitude
                event[10], # Longitude
                event[11]  # Event Type
            ])

        cursor.close()
        conn.close()

        # Prepare response
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename=events_report_{driver_info["name"]}_{start_date}_to_{end_date}.csv'
            }
        )

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

################################## vehicles ######################
@app.route('/vehicles')
def vehicles():
    if not session.get('logged_in'):
        return redirect(url_for('index'))
    return render_template('vehicles.html')

@app.route('/get_vehicles')
def get_vehicles():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        page = int(request.args.get('page', 1))
        per_page = 10
        offset = (page - 1) * per_page

        search_ba_number = request.args.get('search_ba_number', '').strip()
        make = request.args.get('make', '').strip()
        type_filter = request.args.get('type', '').strip()
        unit = request.args.get('unit', '').strip()

        conn = get_db_connection()
        cursor = conn.cursor()

        # Build WHERE clause
        where_conditions = []
        params = []

        if search_ba_number:
            where_conditions.append("BA_number LIKE %s")
            params.append(f"%{search_ba_number}%")
        if make:
            where_conditions.append("make = %s")
            params.append(make)
        if type_filter:
            where_conditions.append("type = %s")
            params.append(type_filter)
        if unit:
            where_conditions.append("unit = %s")
            params.append(unit)

        where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""

        # Get total count
        count_query = f"SELECT COUNT(*) FROM vehicles{where_clause}"
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]

        # Get vehicles with pagination
        query = f"""
            SELECT id, BA_number, make, type, model, total_milage, unit, maintaining_workshop
            FROM vehicles{where_clause}
            ORDER BY id
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """
        cursor.execute(query, params + [offset, per_page])

        vehicles = []
        for row in cursor.fetchall():
            vehicles.append({
                'id': row[0],
                'ba_number': row[1],
                'make': row[2],
                'type': row[3],
                'model': row[4],
                'total_milage': row[5],
                'unit': row[6],
                'maintaining_workshop': row[7]
            })

        total_pages = (total_count + per_page - 1) // per_page

        cursor.close()
        conn.close()

        return jsonify({
            'vehicles': vehicles,
            'current_page': page,
            'total_pages': total_pages,
            'total_count': total_count
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_vehicle_filters')
def get_vehicle_filters():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get unique makes
        cursor.execute("SELECT DISTINCT make FROM vehicles WHERE make IS NOT NULL ORDER BY make")
        makes = [row[0] for row in cursor.fetchall()]

        # Get unique types
        cursor.execute("SELECT DISTINCT type FROM vehicles WHERE type IS NOT NULL ORDER BY type")
        types = [row[0] for row in cursor.fetchall()]

        # Get unique units
        cursor.execute("SELECT DISTINCT unit FROM vehicles WHERE unit IS NOT NULL ORDER BY unit")
        units = [row[0] for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        return jsonify({
            'makes': makes,
            'types': types,
            'units': units
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/add_vehicle', methods=['POST'])
def add_vehicle():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()

        vehicle_id = data.get('id', '').strip()
        ba_number = data.get('ba_number', '').strip()
        make = data.get('make', '').strip()
        vehicle_type = data.get('type', '').strip()
        model = data.get('model', '').strip()
        total_milage = data.get('total_milage', 0)
        unit = data.get('unit', '').strip() or None
        maintaining_workshop = data.get('maintaining_workshop', '').strip() or None
        maintenance_data = data.get('maintenance_data', [])

        if not vehicle_id or not ba_number or not make or not vehicle_type or not model:
            return jsonify({'success': False, 'message': 'All required fields must be filled'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if vehicle ID already exists
        cursor.execute("SELECT id FROM vehicles WHERE id = %s", (vehicle_id,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Vehicle ID already exists'})

        # Check if BA number already exists
        cursor.execute("SELECT BA_number FROM vehicles WHERE BA_number = %s", (ba_number,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'BA Number already exists'})

        # Insert vehicle
        cursor.execute("""
            INSERT INTO vehicles (id, BA_number, make, type, model, total_milage, unit, maintaining_workshop)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (vehicle_id, ba_number, make, vehicle_type, model, total_milage, unit, maintaining_workshop))

        # Insert maintenance records (removed completion_date and completion_mileage)
        for maintenance in maintenance_data:
            maintenance_type = maintenance.get('type')
            last_done = maintenance.get('last_done') or None
            last_done_mileage = maintenance.get('last_done_mileage') or 0
            periodicity_text = maintenance.get('periodicity')
            time_interval = maintenance.get('time_interval_months')
            distance_interval = maintenance.get('distance_interval_km')

            # Convert periodicity text to number (0=time, 1=distance, 2=both)
            periodicity_map = {'time': 0, 'distance': 1, 'both': 2}
            periodicity = periodicity_map.get(periodicity_text, 0)

            # Calculate next due date and mileage
            next_due = None
            next_due_mileage = None
            if last_done and time_interval and (periodicity == 0 or periodicity == 2):
                from datetime import datetime, timedelta
                last_done_date = datetime.strptime(last_done, '%Y-%m-%d')
                next_due = (last_done_date + timedelta(days=time_interval * 30)).strftime('%Y-%m-%d')
            if last_done_mileage is not None and distance_interval and (periodicity == 1 or periodicity == 2):
                next_due_mileage = last_done_mileage + distance_interval

            cursor.execute("""
                INSERT INTO maintenance_records 
                (vehicle_id, type, last_done, last_done_mileage, next_due, next_due_mileage, periodicity, time_interval_months, distance_interval_km, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (vehicle_id, maintenance_type, last_done, last_done_mileage, next_due, next_due_mileage, periodicity,
                  time_interval, distance_interval, 'not'))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'message': 'Vehicle added successfully'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/get_vehicle_by_id')
def get_vehicle_by_id():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        vehicle_id = request.args.get('vehicle_id', '').strip()
        if not vehicle_id:
            return jsonify({'success': False, 'message': 'Vehicle ID is required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Get vehicle details
        cursor.execute("""
            SELECT id, BA_number, make, type, model, total_milage, unit, maintaining_workshop
            FROM vehicles WHERE id = %s
        """, (vehicle_id,))

        vehicle_row = cursor.fetchone()
        if not vehicle_row:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Vehicle not found'})

        vehicle = {
            'id': vehicle_row[0],
            'ba_number': vehicle_row[1],
            'make': vehicle_row[2],
            'type': vehicle_row[3],
            'model': vehicle_row[4],
            'total_milage': vehicle_row[5],
            'unit': vehicle_row[6],
            'maintaining_workshop': vehicle_row[7]
        }

        # Get latest maintenance records (only those with status 'not' or the most recent for each type)
        cursor.execute("""
            SELECT m1.type, m1.last_done, m1.last_done_mileage, m1.next_due, m1.next_due_mileage, m1.periodicity, 
                   m1.time_interval_months, m1.distance_interval_km, m1.status, m1.completion_date, m1.completion_mileage
            FROM maintenance_records m1
            WHERE m1.vehicle_id = %s AND m1.id IN (
                SELECT MAX(m2.id) 
                FROM maintenance_records m2 
                WHERE m2.vehicle_id = %s AND m2.type = m1.type 
            )
        """, (vehicle_id, vehicle_id))

        maintenance_records = []
        for row in cursor.fetchall():
            maintenance_records.append({
                'type': row[0],
                'last_done': row[1],
                'last_done_mileage': row[2],
                'next_due': row[3],
                'next_due_mileage': row[4],
                'periodicity': row[5],
                'time_interval_months': row[6],
                'distance_interval_km': row[7],
                'status': row[8],
                'completion_date': row[9],
                'completion_mileage': row[10]
            })

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'vehicle': vehicle,
            'maintenance_records': maintenance_records
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/get_vehicle_by_ba_number')
def get_vehicle_by_ba_number():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        ba_number = request.args.get('ba_number', '').strip()
        if not ba_number:
            return jsonify({'success': False, 'message': 'BA Number is required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Get vehicle details
        cursor.execute("""
            SELECT id, BA_number, make, type, model, total_milage, unit, maintaining_workshop
            FROM vehicles WHERE BA_number = %s
        """, (ba_number,))

        vehicle_row = cursor.fetchone()
        if not vehicle_row:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Vehicle not found'})

        vehicle = {
            'id': vehicle_row[0],
            'ba_number': vehicle_row[1],
            'make': vehicle_row[2],
            'type': vehicle_row[3],
            'model': vehicle_row[4],
            'total_milage': vehicle_row[5],
            'unit': vehicle_row[6],
            'maintaining_workshop': vehicle_row[7]
        }

        # Get latest maintenance records (only those with status 'not' or the most recent for each type)
        cursor.execute("""
            SELECT m1.type, m1.last_done, m1.last_done_mileage, m1.next_due, m1.next_due_mileage, m1.periodicity, 
                   m1.time_interval_months, m1.distance_interval_km, m1.status, m1.completion_date, m1.completion_mileage
            FROM maintenance_records m1
            WHERE m1.vehicle_id = %s AND m1.id IN (
                SELECT MAX(m2.id) 
                FROM maintenance_records m2 
                WHERE m2.vehicle_id = %s AND m2.type = m1.type 
            )
        """, (vehicle['id'], vehicle['id']))

        maintenance_records = []
        for row in cursor.fetchall():
            maintenance_records.append({
                'type': row[0],
                'last_done': row[1],
                'last_done_mileage': row[2],
                'next_due': row[3],
                'next_due_mileage': row[4],
                'periodicity': row[5],
                'time_interval_months': row[6],
                'distance_interval_km': row[7],
                'status': row[8],
                'completion_date': row[9],
                'completion_mileage': row[10]
            })

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'vehicle': vehicle,
            'maintenance_records': maintenance_records
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/update_vehicle', methods=['POST'])
def update_vehicle():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()

        original_vehicle_id = data.get('vehicle_id', '').strip()
        new_vehicle_id = data.get('new_vehicle_id', '').strip()
        ba_number = data.get('ba_number', '').strip()
        make = data.get('make', '').strip()
        vehicle_type = data.get('type', '').strip()
        model = data.get('model', '').strip()
        total_milage = data.get('total_milage', 0)
        unit = data.get('unit', '').strip() or None
        maintaining_workshop = data.get('maintaining_workshop', '').strip() or None
        maintenance_data = data.get('maintenance_data', [])

        if not original_vehicle_id or not new_vehicle_id or not ba_number or not make or not vehicle_type or not model:
            return jsonify({'success': False, 'message': 'All required fields must be filled'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if the new vehicle ID already exists (if different from original)
        if original_vehicle_id != new_vehicle_id:
            cursor.execute("SELECT id FROM vehicles WHERE id = %s", (new_vehicle_id,))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'message': 'New Vehicle ID already exists'})

        # Check if BA number exists for other vehicles
        cursor.execute("SELECT id FROM vehicles WHERE BA_number = %s AND id != %s", (ba_number, original_vehicle_id))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'BA Number already exists for another vehicle'})

        if original_vehicle_id != new_vehicle_id:
            # Insert new vehicle record with updated data
            cursor.execute("""
                INSERT INTO vehicles (id, BA_number, make, type, model, total_milage, unit, maintaining_workshop)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (new_vehicle_id, ba_number, make, vehicle_type, model, total_milage, unit, maintaining_workshop))

            # Update related tables to point to new vehicle_id
            cursor.execute("UPDATE GPSData SET vehicle_id = %s WHERE vehicle_id = %s", (new_vehicle_id, original_vehicle_id))
            cursor.execute("UPDATE notifications SET vehicle_id = %s WHERE vehicle_id = %s", (new_vehicle_id, original_vehicle_id))
            cursor.execute("UPDATE events SET vehicle_id = %s WHERE vehicle_id = %s", (new_vehicle_id, original_vehicle_id))
            cursor.execute("UPDATE settings SET vehicle_id = %s WHERE vehicle_id = %s", (new_vehicle_id, original_vehicle_id))
            cursor.execute("UPDATE maintenance_records SET vehicle_id = %s WHERE vehicle_id = %s", (new_vehicle_id, original_vehicle_id))

            # Delete the old vehicle record
            cursor.execute("DELETE FROM vehicles WHERE id = %s", (original_vehicle_id,))

        else:
            # Same ID, just update the vehicle record
            cursor.execute("""
                UPDATE vehicles 
                SET BA_number = %s, make = %s, type = %s, model = %s, total_milage = %s, unit = %s, maintaining_workshop = %s
                WHERE id = %s
            """, (ba_number, make, vehicle_type, model, total_milage, unit, maintaining_workshop, original_vehicle_id))

        # For maintenance updates, only update the latest record for each type or create new ones if changes made
        for maintenance in maintenance_data:
            maintenance_type = maintenance.get('type')
            last_done = maintenance.get('last_done') or None
            last_done_mileage = maintenance.get('last_done_mileage') or 0
            periodicity_text = maintenance.get('periodicity')
            time_interval = maintenance.get('time_interval_months')
            distance_interval = maintenance.get('distance_interval_km')

            # Convert periodicity text to number
            periodicity_map = {'time': 0, 'distance': 1, 'both': 2}
            periodicity = periodicity_map.get(periodicity_text, 0)

            # Calculate next due date and mileage
            next_due = None
            next_due_mileage = None
            if last_done and time_interval and (periodicity == 0 or periodicity == 2):
                from datetime import datetime, timedelta
                last_done_date = datetime.strptime(last_done, '%Y-%m-%d')
                next_due = (last_done_date + timedelta(days=time_interval * 30)).strftime('%Y-%m-%d')
            if last_done_mileage is not None and distance_interval and (periodicity == 1 or periodicity == 2):
                next_due_mileage = last_done_mileage + distance_interval

            # Get the latest maintenance record for this type
            cursor.execute("""
                SELECT id, last_done FROM maintenance_records 
                WHERE vehicle_id = %s AND type = %s 
                ORDER BY id DESC
            """, (new_vehicle_id, maintenance_type))

            latest_record = cursor.fetchone()

            if latest_record:
                # Check if the last_done date has changed
                existing_last_done = latest_record[1]
                existing_date_str = str(existing_last_done).split(' ')[0] if existing_last_done else ''
                if existing_date_str != last_done:
                    # Create a new record if last_done changed
                    cursor.execute("""
                        INSERT INTO maintenance_records 
                        (vehicle_id, type, last_done, last_done_mileage, next_due, next_due_mileage, periodicity, time_interval_months, distance_interval_km, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (new_vehicle_id, maintenance_type, last_done, last_done_mileage, next_due, next_due_mileage, periodicity,
                          time_interval, distance_interval, 'not'))
                else:
                    # Update the existing latest record if only other details changed
                    cursor.execute("""
                        UPDATE maintenance_records  
                        SET last_done_mileage = %s, next_due = %s, next_due_mileage = %s, periodicity = %s, time_interval_months = %s, 
                            distance_interval_km = %s
                        WHERE id = %s
                    """, (last_done_mileage, next_due, next_due_mileage, periodicity, time_interval, distance_interval,
                          latest_record[0]))
            else:
                # No existing record, create new one
                cursor.execute("""
                    INSERT INTO maintenance_records 
                    (vehicle_id, type, last_done, last_done_mileage, next_due, next_due_mileage, periodicity, time_interval_months, distance_interval_km, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (new_vehicle_id, maintenance_type, last_done, last_done_mileage, next_due, next_due_mileage, periodicity,
                      time_interval, distance_interval, 'not'))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'message': 'Vehicle updated successfully'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/remove_vehicle', methods=['POST'])
def remove_vehicle():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        vehicle_id = data.get('vehicle_id', '').strip()

        if not vehicle_id:
            return jsonify({'success': False, 'message': 'Vehicle ID is required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if vehicle exists
        cursor.execute("SELECT id FROM vehicles WHERE id = %s", (vehicle_id,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Vehicle not found'})

        # Delete related records in correct order
        cursor.execute("DELETE FROM settings WHERE vehicle_id = %s", (vehicle_id,))
        cursor.execute("DELETE FROM events WHERE vehicle_id = %s", (vehicle_id,))
        cursor.execute("DELETE FROM notifications WHERE vehicle_id = %s", (vehicle_id,))
        cursor.execute("DELETE FROM maintenance_records WHERE vehicle_id = %s", (vehicle_id,))
        cursor.execute("DELETE FROM GPSData WHERE vehicle_id = %s", (vehicle_id,))
        cursor.execute("DELETE FROM vehicles WHERE id = %s", (vehicle_id,))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'message': 'Vehicle and all related data removed successfully'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/generate_vehicle_maintenance_report')
def generate_vehicle_maintenance_report():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        vehicle_id = request.args.get('vehicle_id', '').strip()

        if not vehicle_id:
            return jsonify({'success': False, 'message': 'Vehicle ID is required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Get vehicle details
        cursor.execute("""
            SELECT id, BA_number, make, type, model, total_milage, unit, maintaining_workshop
            FROM vehicles WHERE id = %s
        """, (vehicle_id,))

        vehicle_row = cursor.fetchone()
        if not vehicle_row:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Vehicle not found'})

        vehicle = {
            'id': vehicle_row[0],
            'ba_number': vehicle_row[1],
            'make': vehicle_row[2],
            'type': vehicle_row[3],
            'model': vehicle_row[4],
            'total_milage': vehicle_row[5] or 0,
            'unit': vehicle_row[6],
            'maintaining_workshop': vehicle_row[7]
        }

        # Get latest maintenance records
        cursor.execute("""
            SELECT m1.type, m1.last_done, m1.last_done_mileage, m1.next_due, m1.next_due_mileage, m1.periodicity, 
                   m1.time_interval_months, m1.distance_interval_km, m1.status
            FROM maintenance_records m1
            WHERE m1.vehicle_id = %s AND m1.id IN (
                SELECT MAX(m2.id) 
                FROM maintenance_records m2 
                WHERE m2.vehicle_id = %s AND m2.type = m1.type 
            )
            ORDER BY m1.type
        """, (vehicle_id, vehicle_id))

        maintenance_report = []
        current_date = datetime.now()
        current_mileage = vehicle['total_milage']

        for row in cursor.fetchall():
            record = {
                'type': row[0],
                'last_done': row[1].strftime('%Y-%m-%d') if row[1] else 'N/A',
                'last_done_mileage': row[2] or 0,
                'next_due': row[3].strftime('%Y-%m-%d') if row[3] else 'N/A',
                'next_due_mileage': row[4] or 0,
                'periodicity': row[5],
                'time_interval_months': row[6],
                'distance_interval_km': row[7],
                'status': 'On Schedule',
                'progress_percentage': 0
            }

            # Calculate status and progress based on periodicity
            if row[8] == 'done':
                record['status'] = 'done'
                record['progress_percentage'] = 100
            else:
                # Initialize variables for calculation
                time_progress = 0
                distance_progress = 0
                is_overdue = False
                is_due_soon = False

                # Time-based calculation
                if record['periodicity'] in [0, 2] and row[1] and record['time_interval_months']:
                    try:
                        last_done_date = row[1]
                        months_passed = (current_date - last_done_date).days / 30.44
                        time_progress = (months_passed / record['time_interval_months']) * 100

                        if months_passed >= record['time_interval_months']:
                            is_overdue = True
                        elif months_passed >= record['time_interval_months'] * 0.8:
                            is_due_soon = True
                    except Exception as e:
                        print(f"Time calculation error: {e}")

                # Distance-based calculation
                if record['periodicity'] in [1, 2] and record['last_done_mileage'] is not None and record[
                    'distance_interval_km']:
                    mileage_since = current_mileage - record['last_done_mileage']
                    distance_progress = (mileage_since / record['distance_interval_km']) * 100

                    if mileage_since >= record['distance_interval_km']:
                        is_overdue = True
                    elif mileage_since >= record['distance_interval_km'] * 0.8:
                        is_due_soon = True

                # Determine final progress and status
                if record['periodicity'] == 0:  # Time only
                    record['progress_percentage'] = min(time_progress, 100)
                elif record['periodicity'] == 1:  # Distance only
                    record['progress_percentage'] = min(distance_progress, 100)
                elif record['periodicity'] == 2:  # Both
                    record['progress_percentage'] = min(max(time_progress, distance_progress), 100)

                # Set status
                if is_overdue:
                    record['status'] = 'Overdue'
                elif is_due_soon:
                    record['status'] = 'Due Soon'
                else:
                    record['status'] = 'On Schedule'

            maintenance_report.append(record)

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'maintenance_report': maintenance_report
        })

    except Exception as e:
        print(f"Error in maintenance report: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/set_speed_limit', methods=['POST'])
def set_speed_limit():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()

        vehicle_id = data.get('vehicle_id', '').strip()
        road_type = data.get('road_type', '').strip()
        speed_limit = data.get('speed_limit')

        if not vehicle_id or not road_type or speed_limit is None:
            return jsonify({'success': False, 'message': 'Vehicle ID, road type, and speed limit are required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if entry exists for this vehicle and road_type
        cursor.execute("""
            SELECT id FROM settings WHERE vehicle_id = %s AND road_type = %s
        """, (vehicle_id, road_type))

        existing = cursor.fetchone()

        if existing:
            # Update existing record
            cursor.execute("""
                UPDATE settings SET speed_limit = %s
                WHERE vehicle_id = %s AND road_type = %s
            """, (speed_limit, vehicle_id, road_type))
            message = f'Speed limit updated to {speed_limit} km/h for {road_type}'
        else:
            # Insert new record
            cursor.execute("""
                INSERT INTO settings (vehicle_id, speed_limit, road_type)
                VALUES (%s, %s, %s)
            """, (vehicle_id, speed_limit, road_type))
            message = f'Speed limit set to {speed_limit} km/h for {road_type}'

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'message': message})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

############################# admins page ######################

@app.route('/change_admin_password', methods=['POST'])
def change_admin_password():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        admin_id = data.get('admin_id')
        new_password = data.get('new_password', '').strip()

        if not admin_id or not new_password:
            return jsonify({'success': False, 'message': 'Admin ID and new password are required'})

        if len(new_password) < 6:
            return jsonify({'success': False, 'message': 'Password must be at least 6 characters long'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if admin exists
        cursor.execute("SELECT id FROM Admins WHERE id = %s", (admin_id,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Admin not found'})

        # Hash the new password
        password_hash = hashlib.sha256(new_password.encode()).hexdigest()

        # Update password
        cursor.execute("""
            UPDATE Admins 
            SET password_hash = %s
            WHERE id = %s
        """, (password_hash, admin_id))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'message': 'Password changed successfully'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/get_admins')
def get_admins():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        page = int(request.args.get('page', 1))
        search_name = request.args.get('search_name', '').strip()
        per_page = 10  # Number of records per page

        conn = get_db_connection()
        cursor = conn.cursor()

        # Build query with search filter
        where_clause = ""
        params = []

        if search_name:
            where_clause = "WHERE full_name LIKE %s"
            params.append(f"%{search_name}%")

        # Get total count
        count_query = f"SELECT COUNT(*) FROM Admins {where_clause}"
        cursor.execute(count_query, params)
        total_records = cursor.fetchone()[0]
        total_pages = (total_records + per_page - 1) // per_page

        # Get paginated data (removed password_hash from SELECT)
        offset = (page - 1) * per_page
        data_query = f"""
            SELECT id, full_name, Username 
            FROM Admins {where_clause}
            ORDER BY id
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """
        cursor.execute(data_query, params + [offset, per_page])

        admins = []
        for row in cursor.fetchall():
            admins.append({
                'id': row[0],
                'full_name': row[1],
                'username': row[2]
                # Removed password_hash
            })

        cursor.close()
        conn.close()

        return jsonify({
            'admins': admins,
            'current_page': page,
            'total_pages': total_pages,
            'total_records': total_records
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/add_admin', methods=['POST'])
def add_admin():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()

        full_name = data.get('full_name', '').strip()
        username = data.get('username', '').strip()
        password = data.get('password', '').strip()

        if not full_name or not username or not password:
            return jsonify({'success': False, 'message': 'All fields are required'})

        if len(password) < 6:
            return jsonify({'success': False, 'message': 'Password must be at least 6 characters long'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if username already exists
        cursor.execute("SELECT Username FROM Admins WHERE Username = %s", (username,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Username already exists'})

        # Hash the password
        password_hash = hashlib.sha256(password.encode()).hexdigest()

        # Insert new admin
        cursor.execute("""
            INSERT INTO Admins (full_name, Username, password_hash)
            VALUES (%s, %s, %s)
        """, (full_name, username, password_hash))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'message': 'Admin added successfully'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/get_admin_by_id')
def get_admin_by_id():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        admin_id = request.args.get('admin_id')

        if not admin_id:
            return jsonify({'success': False, 'message': 'Admin ID is required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, full_name, Username FROM Admins WHERE id = %s", (admin_id,))
        row = cursor.fetchone()

        cursor.close()
        conn.close()

        if row:
            admin = {
                'id': row[0],
                'full_name': row[1],
                'username': row[2]
            }
            return jsonify({'admin': admin})
        else:
            return jsonify({'success': False, 'message': 'Admin not found'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/get_admin_by_username')
def get_admin_by_username():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        username = request.args.get('username', '').strip()

        if not username:
            return jsonify({'success': False, 'message': 'Username is required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, full_name, Username FROM Admins WHERE Username = %s", (username,))
        row = cursor.fetchone()

        cursor.close()
        conn.close()

        if row:
            admin = {
                'id': row[0],
                'full_name': row[1],
                'username': row[2]
            }
            return jsonify({'admin': admin})
        else:
            return jsonify({'success': False, 'message': 'Admin not found'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/update_admin', methods=['POST'])
def update_admin():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()

        admin_id = data.get('admin_id')
        full_name = data.get('full_name', '').strip()
        username = data.get('username', '').strip()

        if not admin_id or not full_name or not username:
            return jsonify({'success': False, 'message': 'Admin ID, Full Name, and Username are required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if admin exists
        cursor.execute("SELECT id FROM Admins WHERE id = %s", (admin_id,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Admin not found'})

        # Check if username is taken by another admin
        cursor.execute("SELECT id FROM Admins WHERE Username = %s AND id != %s", (username, admin_id))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Username already exists'})

        # Update admin (without password)
        cursor.execute("""
            UPDATE Admins 
            SET full_name = %s, Username = %s
            WHERE id = %s
        """, (full_name, username, admin_id))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'message': 'Admin updated successfully'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/remove_admin', methods=['POST'])
def remove_admin():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        admin_id = data.get('admin_id')

        if not admin_id:
            return jsonify({'success': False, 'message': 'Admin ID is required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if admin exists
        cursor.execute("SELECT id FROM Admins WHERE id = %s", (admin_id,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Admin not found'})

        # Prevent deletion if this is the only admin (optional safety check)
        cursor.execute("SELECT COUNT(*) FROM Admins")
        admin_count = cursor.fetchone()[0]

        if admin_count <= 1:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Cannot delete the last admin account'})

        # Prevent self-deletion (optional safety check)
        current_admin_id = session.get('admin_id')  # Assuming you store admin_id in session
        if str(admin_id) == str(current_admin_id):
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Cannot delete your own account'})

        # Delete admin
        cursor.execute("DELETE FROM Admins WHERE id = %s", (admin_id,))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'message': 'Admin removed successfully'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

################################# Live Tracking Page ##################################

@app.route('/api/vehicles')
def get_vehicles_for_live():
    """Get all vehicles for dropdown selection"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, BA_number FROM vehicles ORDER BY BA_number")
        vehicles = cursor.fetchall()

        vehicles_list = []
        for vehicle in vehicles:
            vehicles_list.append({
                'id': vehicle[0],
                'BA_number': vehicle[1]
            })

        return jsonify({
            'success': True,
            'vehicles': vehicles_list
        })

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch vehicles'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/vehicle-by-ba/<ba_number>')
def get_vehicle_by_ba(ba_number):
    """Get vehicle ID by BA number"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, BA_number FROM vehicles WHERE BA_number = %s", (ba_number,))
        vehicle = cursor.fetchone()

        if vehicle:
            return jsonify({
                'success': True,
                'vehicle': {
                    'id': vehicle[0],
                    'BA_number': vehicle[1]
                }
            })
        else:
            return jsonify({'success': False, 'message': 'Vehicle not found'})

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch vehicle'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/vehicle-details/<vehicle_id>')
def get_vehicle_details(vehicle_id):
    """Get detailed vehicle information"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, BA_number, make, type, model, total_milage, unit, maintaining_workshop
            FROM vehicles 
            WHERE id = %s
        """, (vehicle_id,))

        vehicle = cursor.fetchone()

        if vehicle:
            return jsonify({
                'success': True,
                'vehicle': {
                    'id': vehicle[0],
                    'BA_number': vehicle[1],
                    'make': vehicle[2],
                    'type': vehicle[3],
                    'model': vehicle[4],
                    'total_milage': vehicle[5],
                    'unit': vehicle[6],
                    'maintaining_workshop': vehicle[7]
                }
            })
        else:
            return jsonify({'success': False, 'message': 'Vehicle not found'})

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch vehicle details'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/live-gps-data/<vehicle_id>')
def get_live_gps_data(vehicle_id):
    """Get live GPS data for a vehicle from a specific time onwards - OPTIMIZED"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    # Get the 'since' parameter from query string (ISO datetime string)
    since_param = request.args.get('since')
    if not since_param:
        return jsonify({'success': False, 'message': 'Missing since parameter'})

    conn = None
    cursor = None

    try:
        # Parse the since parameter
        since_datetime = datetime.fromisoformat(since_param.replace('Z', '+00:00'))

        conn = get_db_connection()
        cursor = conn.cursor()

        # Optimized query with index usage and limit for performance
        cursor.execute("""
            SELECT TOP 1000 g.id, g.vehicle_id, g.driver_id, g.timestamp, g.lat, g.lon, g.speed,
                   d.name as driver_name, d.rank as driver_rank, d.army_number as driver_army_number, d.unit as driver_unit
            FROM GPSData_live g WITH (INDEX(IX_GPSData_live_Vehicle_Timestamp))
            LEFT JOIN drivers d ON g.driver_id = d.id
            WHERE g.vehicle_id = %s AND g.timestamp > %s
            ORDER BY g.timestamp ASC
        """, (vehicle_id, since_datetime))

        gps_data = cursor.fetchall()

        if gps_data:
            data_list = []
            for row in gps_data:
                # Validate coordinates
                lat = float(row[4]) if row[4] is not None else None
                lon = float(row[5]) if row[5] is not None else None

                if lat is None or lon is None or lat == 0 or lon == 0:
                    continue  # Skip invalid coordinates

                data_list.append({
                    'id': row[0],
                    'vehicle_id': row[1],
                    'driver_id': row[2],
                    'timestamp': row[3].isoformat() if row[3] else None,
                    'lat': lat,
                    'lon': lon,
                    'speed': float(row[6]) if row[6] is not None else 0,
                    'driver_name': row[7],
                    'driver_rank': row[8],
                    'driver_army_number': row[9],
                    'driver_unit': row[10]
                })

            return jsonify({
                'success': True,
                'data': data_list,
                'count': len(data_list)
            })
        else:
            return jsonify({'success': False, 'message': 'No GPS data found'})

    except ValueError as ve:
        return jsonify({'success': False, 'message': 'Invalid datetime format'})
    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch GPS data'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/live-vehicles')
def get_live_vehicles():
    """Get all vehicles that have transmitted GPS data in the last 30 seconds"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get current time minus 30 seconds
        cutoff_time = datetime.now() - timedelta(seconds=30)

        # Find all vehicles with recent GPS data
        cursor.execute("""
            SELECT DISTINCT g.vehicle_id, v.BA_number, 
                   MAX(g.timestamp) as last_update,
                   COUNT(*) as point_count
            FROM GPSData_live g WITH (INDEX(IX_GPSData_live_Timestamp))
            JOIN vehicles v ON g.vehicle_id = v.id
            WHERE g.timestamp >= %s
            GROUP BY g.vehicle_id, v.BA_number
            ORDER BY last_update DESC
        """, (cutoff_time,))

        live_vehicles = cursor.fetchall()

        if live_vehicles:
            vehicles_list = []
            for vehicle in live_vehicles:
                vehicles_list.append({
                    'vehicle_id': vehicle[0],
                    'BA_number': vehicle[1],
                    'last_update': vehicle[2].isoformat() if vehicle[2] else None,
                    'point_count': vehicle[3]
                })

            return jsonify({
                'success': True,
                'vehicles': vehicles_list,
                'count': len(vehicles_list)
            })
        else:
            return jsonify({'success': True, 'vehicles': [], 'count': 0})

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch live vehicles'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/all-live-gps-data')
def get_all_live_gps_data():
    """Get live GPS data for all vehicles from a specific time onwards - OPTIMIZED"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    # Get the 'since' parameter from query string (ISO datetime string)
    since_param = request.args.get('since')
    if not since_param:
        return jsonify({'success': False, 'message': 'Missing since parameter'})

    conn = None
    cursor = None

    try:
        # Parse the since parameter
        since_datetime = datetime.fromisoformat(since_param.replace('Z', '+00:00'))

        conn = get_db_connection()
        cursor = conn.cursor()

        # Optimized query to get all vehicles' GPS data at once
        cursor.execute("""
            SELECT TOP 5000 g.vehicle_id, g.timestamp, g.lat, g.lon, g.speed,
                   d.name as driver_name, d.rank as driver_rank, 
                   d.army_number as driver_army_number, d.unit as driver_unit
            FROM GPSData_live g WITH (INDEX(IX_GPSData_live_Timestamp))
            LEFT JOIN drivers d ON g.driver_id = d.id
            WHERE g.timestamp > %s 
            AND g.lat IS NOT NULL AND g.lon IS NOT NULL 
            AND g.lat != 0 AND g.lon != 0
            ORDER BY g.vehicle_id, g.timestamp ASC
        """, (since_datetime,))

        gps_data = cursor.fetchall()

        # Group data by vehicle_id
        vehicles_data = {}

        for row in gps_data:
            vehicle_id = row[0]

            if vehicle_id not in vehicles_data:
                vehicles_data[vehicle_id] = []

            vehicles_data[vehicle_id].append({
                'timestamp': row[1].isoformat() if row[1] else None,
                'lat': float(row[2]),
                'lon': float(row[3]),
                'speed': float(row[4]) if row[4] is not None else 0,
                'driver_name': row[5],
                'driver_rank': row[6],
                'driver_army_number': row[7],
                'driver_unit': row[8]
            })

        return jsonify({
            'success': True,
            'vehicles': vehicles_data,
            'total_points': len(gps_data),
            'vehicle_count': len(vehicles_data)
        })

    except ValueError as ve:
        return jsonify({'success': False, 'message': 'Invalid datetime format'})
    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch GPS data'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/latest-position/<vehicle_id>')
def get_latest_position(vehicle_id):
    """Get the latest GPS position for a vehicle - OPTIMIZED"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT TOP 1 g.timestamp, g.lat, g.lon, g.speed,
                   d.name as driver_name, d.rank as driver_rank, d.army_number as driver_army_number
            FROM GPSData_live g WITH (INDEX(IX_GPSData_live_Vehicle_Timestamp))
            LEFT JOIN drivers d ON g.driver_id = d.id
            WHERE g.vehicle_id = %s
            ORDER BY g.timestamp DESC
        """, (vehicle_id,))

        position = cursor.fetchone()

        if position:
            # Check if position is recent (within last 2 minutes)
            last_update = position[0]
            time_diff = datetime.now() - last_update if last_update else timedelta(hours=1)
            is_live = time_diff.total_seconds() <= 120  # 2 minutes

            return jsonify({
                'success': True,
                'position': {
                    'timestamp': position[0].isoformat() if position[0] else None,
                    'lat': float(position[1]) if position[1] else None,
                    'lon': float(position[2]) if position[2] else None,
                    'speed': float(position[3]) if position[3] else 0,
                    'driver_name': position[4],
                    'driver_rank': position[5],
                    'driver_army_number': position[6],
                    'is_live': is_live,
                    'age_seconds': int(time_diff.total_seconds())
                }
            })
        else:
            return jsonify({'success': False, 'message': 'No position data found'})

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch latest position'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/recent-gps-batch/<vehicle_id>')
def get_recent_gps_batch(vehicle_id):
    """Get recent GPS data in batches for better performance"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    # Get parameters
    minutes_back = int(request.args.get('minutes', 5))  # Default 5 minutes
    max_points = int(request.args.get('limit', 100))  # Default 100 points

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Calculate time cutoff
        cutoff_time = datetime.now() - timedelta(minutes=minutes_back)

        cursor.execute(f"""
            SELECT TOP {max_points} g.timestamp, g.lat, g.lon, g.speed,
                   d.name as driver_name, d.rank as driver_rank, 
                   d.army_number as driver_army_number, d.unit as driver_unit
            FROM GPSData_live g WITH (INDEX(IX_GPSData_live_Vehicle_Timestamp))
            LEFT JOIN drivers d ON g.driver_id = d.id
            WHERE g.vehicle_id = %s AND g.timestamp >= %s
            AND g.lat IS NOT NULL AND g.lon IS NOT NULL
            ORDER BY g.timestamp ASC
        """, (vehicle_id, cutoff_time))

        gps_data = cursor.fetchall()

        if gps_data:
            data_list = []
            for row in gps_data:
                data_list.append({
                    'timestamp': row[0].isoformat() if row[0] else None,
                    'lat': float(row[1]),
                    'lon': float(row[2]),
                    'speed': float(row[3]) if row[3] is not None else 0,
                    'driver_name': row[4],
                    'driver_rank': row[5],
                    'driver_army_number': row[6],
                    'driver_unit': row[7]
                })

            return jsonify({
                'success': True,
                'data': data_list,
                'count': len(data_list),
                'timespan_minutes': minutes_back
            })
        else:
            return jsonify({'success': False, 'message': 'No recent GPS data found'})

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch recent GPS data'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/vehicle-status/<vehicle_id>')
def get_vehicle_status(vehicle_id):
    """Get real-time status of a specific vehicle"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get latest position and determine status
        cursor.execute("""
            SELECT TOP 1 g.timestamp, g.lat, g.lon, g.speed, v.BA_number,
                   d.name as driver_name
            FROM GPSData_live g WITH (INDEX(IX_GPSData_live_Vehicle_Timestamp))
            JOIN vehicles v ON g.vehicle_id = v.id
            LEFT JOIN drivers d ON g.driver_id = d.id
            WHERE g.vehicle_id = %s
            ORDER BY g.timestamp DESC
        """, (vehicle_id,))

        latest = cursor.fetchone()

        if latest:
            last_update = latest[0]
            time_diff = datetime.now() - last_update if last_update else timedelta(hours=1)
            age_seconds = int(time_diff.total_seconds())

            # Determine status based on data age
            if age_seconds <= 10:
                status = 'live'
                status_text = 'Live'
            elif age_seconds <= 60:
                status = 'recent'
                status_text = f'Last seen {age_seconds}s ago'
            elif age_seconds <= 300:
                status = 'offline'
                status_text = f'Offline ({age_seconds // 60}m ago)'
            else:
                status = 'offline'
                status_text = f'Offline (>{age_seconds // 60}m ago)'

            return jsonify({
                'success': True,
                'vehicle_id': vehicle_id,
                'BA_number': latest[4],
                'status': status,
                'status_text': status_text,
                'last_update': last_update.isoformat() if last_update else None,
                'age_seconds': age_seconds,
                'last_position': {
                    'lat': float(latest[1]) if latest[1] else None,
                    'lon': float(latest[2]) if latest[2] else None,
                    'speed': float(latest[3]) if latest[3] else 0
                },
                'driver_name': latest[5]
            })
        else:
            return jsonify({
                'success': True,
                'vehicle_id': vehicle_id,
                'status': 'no_data',
                'status_text': 'No GPS data available',
                'last_update': None,
                'age_seconds': None,
                'last_position': None,
                'driver_name': None
            })

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch vehicle status'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/speed-limit/<vehicle_id>')
def get_speed_limit(vehicle_id):
    """Get speed limit settings for a vehicle"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT speed_limit, road_type
            FROM settings
            WHERE vehicle_id = %s
        """, (vehicle_id,))

        settings = cursor.fetchone()

        if settings:
            return jsonify({
                'success': True,
                'speed_limit': float(settings[0]) if settings[0] else 60.0,
                'road_type': settings[1] if settings[1] else 'urban'
            })
        else:
            return jsonify({
                'success': True,
                'speed_limit': 60.0,
                'road_type': 'urban'
            })

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch speed limit'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

###!!!!!!!!!!!!!!!!!!!!! Performance Monitoring Endpoints !!!!!!!!!!!!!!!!!!!############

@app.route('/api/system-health')
def get_system_health():
    """Get system health metrics for monitoring"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get database statistics
        cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM GPSData_live WHERE timestamp >= DATEADD(minute, -5, GETDATE())) as recent_gps_points,
                (SELECT COUNT(DISTINCT vehicle_id) FROM GPSData_live WHERE timestamp >= DATEADD(minute, -1, GETDATE())) as active_vehicles,
                (SELECT COUNT(*) FROM vehicles) as total_vehicles,
                (SELECT TOP 1 timestamp FROM GPSData_live ORDER BY timestamp DESC) as latest_gps_timestamp
        """)

        stats = cursor.fetchone()

        health_status = {
            'timestamp': datetime.now().isoformat(),
            'database_connected': True,
            'recent_gps_points': stats[0] if stats else 0,
            'active_vehicles_1min': stats[1] if stats else 0,
            'total_vehicles': stats[2] if stats else 0,
            'latest_gps_data': stats[3].isoformat() if stats and stats[3] else None
        }

        # Determine overall health
        if health_status['recent_gps_points'] > 0:
            health_status['status'] = 'healthy'
        elif health_status['active_vehicles_1min'] > 0:
            health_status['status'] = 'warning'
        else:
            health_status['status'] = 'critical'

        return jsonify({
            'success': True,
            'health': health_status
        })

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({
            'success': False,
            'health': {
                'timestamp': datetime.now().isoformat(),
                'database_connected': False,
                'status': 'critical',
                'error': str(e)
            }
        })

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

#####!!!!!!!!!!!!!!!!!!!!!! Additional Utility Functions !!!!!!!!!!!!!!!!!!!!!!##########

def detect_events_from_gps_data(gps_points, vehicle_id, speed_limit=60):
    """
    Enhanced event detection with better validation
    """
    events_detected = []

    for i in range(len(gps_points)):
        current_point = gps_points[i]

        try:
            # Validate point data
            if not all(key in current_point for key in ['lat', 'lon', 'timestamp', 'speed']):
                continue

            current_speed = float(current_point.get('speed', 0))

            # Detect overspeeding
            if current_speed > speed_limit:
                events_detected.append({
                    'vehicle_id': vehicle_id,
                    'driver_id': current_point.get('driver_id'),
                    'timestamp': current_point.get('timestamp'),
                    'lat': current_point.get('lat'),
                    'lon': current_point.get('lon'),
                    'event_type': 'overspeeding',
                    'details': f"Speed: {current_speed:.1f} km/h (Limit: {speed_limit} km/h)"
                })

            # Detect harsh events (need previous point)
            if i > 0:
                prev_point = gps_points[i - 1]
                prev_speed = float(prev_point.get('speed', 0))

                # Calculate time difference in seconds
                try:
                    current_time = datetime.fromisoformat(current_point.get('timestamp').replace('Z', '+00:00'))
                    prev_time = datetime.fromisoformat(prev_point.get('timestamp').replace('Z', '+00:00'))
                    time_diff = (current_time - prev_time).total_seconds()

                    if 0 < time_diff <= 60:  # Valid time difference (between 0 and 60 seconds)
                        # Calculate acceleration (km/h per second)
                        acceleration = (current_speed - prev_speed) / time_diff

                        # Harsh braking (deceleration > 4 km/h per second)
                        if acceleration < -4:
                            events_detected.append({
                                'vehicle_id': vehicle_id,
                                'driver_id': current_point.get('driver_id'),
                                'timestamp': current_point.get('timestamp'),
                                'lat': current_point.get('lat'),
                                'lon': current_point.get('lon'),
                                'event_type': 'harsh_braking',
                                'details': f"Deceleration: {abs(acceleration):.1f} km/h/s"
                            })

                        # Harsh acceleration (acceleration > 4 km/h per second)
                        elif acceleration > 4:
                            events_detected.append({
                                'vehicle_id': vehicle_id,
                                'driver_id': current_point.get('driver_id'),
                                'timestamp': current_point.get('timestamp'),
                                'lat': current_point.get('lat'),
                                'lon': current_point.get('lon'),
                                'event_type': 'harsh_acceleration',
                                'details': f"Acceleration: {acceleration:.1f} km/h/s"
                            })

                except (ValueError, TypeError) as e:
                    # Skip if timestamp parsing fails
                    continue

        except (ValueError, TypeError) as e:
            # Skip invalid data points
            continue

    return events_detected

@app.route('/api/process-events', methods=['POST'])
def process_events():
    """
    Enhanced endpoint to process events for GPS data
    """
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        vehicle_id = data.get('vehicle_id')
        gps_points = data.get('gps_points', [])
        speed_limit = data.get('speed_limit', 60)

        if not vehicle_id or not gps_points:
            return jsonify({'success': False, 'message': 'Missing required parameters'})

        # Detect events
        events = detect_events_from_gps_data(gps_points, vehicle_id, speed_limit)

        if not events:
            return jsonify({
                'success': True,
                'events_detected': 0,
                'events_saved': 0,
                'message': 'No events detected'
            })

        # Save events to database
        conn = None
        cursor = None
        events_saved = 0

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            for event in events:
                try:
                    cursor.execute("""
                        INSERT INTO events (vehicle_id, driver_id, timestamp, lat, lon, event_type)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        event['vehicle_id'],
                        event['driver_id'],
                        event['timestamp'],
                        event['lat'],
                        event['lon'],
                        event['event_type']
                    ))
                    events_saved += 1
                except Exception as e:
                    print(f"Error saving event: {e}")
                    continue

            conn.commit()

            return jsonify({
                'success': True,
                'events_detected': len(events),
                'events_saved': events_saved
            })

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Database error saving events: {e}")
            return jsonify({'success': False, 'message': 'Failed to save events'})

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    except Exception as e:
        print(f"Error processing events: {e}")
        return jsonify({'success': False, 'message': 'Failed to process events'})

#########!!!!!!!!!!!!!!!!!!! Database Maintenance Functions !!!!!!!!!!!!!!!!!!!!!!#########

@app.route('/api/cleanup-old-data', methods=['POST'])
def cleanup_old_data():
    """Clean up old GPS data to maintain performance"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    # Only allow cleanup if user has admin privileges
    # Add your admin check logic here

    conn = None
    cursor = None

    try:
        days_to_keep = int(request.json.get('days', 30))  # Default keep 30 days
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)

        conn = get_db_connection()
        cursor = conn.cursor()

        # Count records to be deleted
        cursor.execute("SELECT COUNT(*) FROM GPSData_live WHERE timestamp < %s", (cutoff_date,))
        records_to_delete = cursor.fetchone()[0]

        # Delete old GPS data
        cursor.execute("DELETE FROM GPSData_live WHERE timestamp < %s", (cutoff_date,))

        # Delete old events
        cursor.execute("DELETE FROM events WHERE timestamp < %s", (cutoff_date,))

        conn.commit()

        return jsonify({
            'success': True,
            'records_deleted': records_to_delete,
            'cutoff_date': cutoff_date.isoformat()
        })

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database cleanup error: {e}")
        return jsonify({'success': False, 'message': 'Failed to cleanup old data'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

############################## route tracking #################################

@app.route('/api/route-data')
def get_route_data_for_route():
    """Get GPS data for route tracking between specified dates"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    vehicle_id = request.args.get('vehicle_id')
    start_time = request.args.get('start')
    end_time = request.args.get('end')

    if not all([vehicle_id, start_time, end_time]):
        return jsonify({'success': False, 'message': 'Missing required parameters'})

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Parse datetime strings to proper format for SQL Server
        try:
            start_dt = datetime.fromisoformat(start_time.replace('T', ' '))
            end_dt = datetime.fromisoformat(end_time.replace('T', ' '))
        except ValueError as e:
            return jsonify({'success': False, 'message': 'Invalid date format'})

        # Get GPS data for the specified time period
        cursor.execute("""
            SELECT lat, lon, speed, timestamp, driver_id
            FROM GPSData 
            WHERE vehicle_id = %s AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp
        """, (vehicle_id, start_dt, end_dt))

        gps_points = cursor.fetchall()

        # Get vehicle information
        cursor.execute("""
            SELECT id, BA_number, make, type, model, total_milage, unit, maintaining_workshop
            FROM vehicles 
            WHERE id = %s
        """, (vehicle_id,))

        vehicle_info = cursor.fetchone()

        # Get unique drivers from GPS data
        if gps_points:
            driver_ids = list(set([point[4] for point in gps_points if point[4]]))

            driver_info = []
            if driver_ids:
                placeholders = ','.join(['%s' for _ in driver_ids])
                cursor.execute(f"""
                    SELECT id, name, rank, army_number, unit
                    FROM drivers 
                    WHERE id IN ({placeholders})
                """, driver_ids)

                drivers = cursor.fetchall()
                for driver in drivers:
                    driver_info.append({
                        'id': driver[0],
                        'name': driver[1],
                        'rank': driver[2],
                        'army_number': driver[3],
                        'unit': driver[4]
                    })
        else:
            driver_info = []

        # Format GPS points
        gps_data = []
        for point in gps_points:
            gps_data.append({
                'lat': float(point[0]),
                'lon': float(point[1]),
                'speed': float(point[2]) if point[2] else 0,
                'timestamp': point[3].isoformat() if point[3] else None,
                'driver_id': point[4]
            })

        # Format vehicle info
        vehicle_data = None
        if vehicle_info:
            vehicle_data = {
                'id': vehicle_info[0],
                'BA_number': vehicle_info[1],
                'make': vehicle_info[2],
                'type': vehicle_info[3],
                'model': vehicle_info[4],
                'total_milage': float(vehicle_info[5]) if vehicle_info[5] else 0,
                'unit': vehicle_info[6],
                'maintaining_workshop': vehicle_info[7]
            }

        return jsonify({
            'success': True,
            'gps_points': gps_data,
            'vehicle_info': vehicle_data,
            'driver_info': driver_info,
            'vehicle_id': vehicle_id  # Add this line to ensure vehicle_id is returned
        })

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch route data'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/events')
def get_events_for_route():
    """Get events (harsh braking, acceleration, overspeeding) for specified vehicle and time period"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    vehicle_id = request.args.get('vehicle_id')
    event_type = request.args.get('event_type')
    start_time = request.args.get('start')
    end_time = request.args.get('end')

    if not all([vehicle_id, event_type, start_time, end_time]):
        return jsonify({'success': False, 'message': 'Missing required parameters'})

    # Validate event type
    valid_events = ['harsh_brake', 'harsh_acceleration', 'overspeeding']
    if event_type not in valid_events:
        return jsonify({'success': False, 'message': 'Invalid event type'})

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Parse datetime strings to proper format for SQL Server
        try:
            start_dt = datetime.fromisoformat(start_time.replace('T', ' '))
            end_dt = datetime.fromisoformat(end_time.replace('T', ' '))
        except ValueError as e:
            return jsonify({'success': False, 'message': 'Invalid date format'})

        cursor.execute("""
            SELECT lat, lon, timestamp, driver_id, event_type
            FROM events 
            WHERE vehicle_id = %s AND event_type = %s AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp
        """, (vehicle_id, event_type, start_dt, end_dt))

        events = cursor.fetchall()

        events_data = []
        for event in events:
            events_data.append({
                'lat': float(event[0]) if event[0] else 0,
                'lon': float(event[1]) if event[1] else 0,
                'timestamp': event[2].isoformat() if event[2] else None,
                'driver_id': event[3],
                'event_type': event[4]
            })

        return jsonify({
            'success': True,
            'events': events_data
        })

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch events'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/vehicles-for-route')
def get_vehicles_for_route():
    """Get all vehicles for route tracking dropdown selection"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, BA_number FROM vehicles ORDER BY BA_number")
        vehicles = cursor.fetchall()

        vehicles_list = []
        for vehicle in vehicles:
            vehicles_list.append({
                'id': vehicle[0],
                'BA_number': vehicle[1]
            })

        return jsonify({
            'success': True,
            'vehicles': vehicles_list
        })

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch vehicles'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/vehicle-by-ba-for-route/<ba_number>')
def get_vehicle_by_ba_for_route(ba_number):
    """Get vehicle ID by BA number for route tracking"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, BA_number FROM vehicles WHERE BA_number = %s", (ba_number,))
        vehicle = cursor.fetchone()

        if vehicle:
            return jsonify({
                'success': True,
                'vehicle': {
                    'id': vehicle[0],
                    'BA_number': vehicle[1]
                }
            })
        else:
            return jsonify({'success': False, 'message': 'Vehicle not found'})

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch vehicle'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/route-tracking')
def route_tracking_page():
    """Render the route tracking page"""
    if not session.get('logged_in'):
        return redirect(url_for('index'))

    return render_template('route_tracking.html')  # You'll need to create this template file

@app.route('/api/driver-details-for-route/<driver_id>')
def get_driver_details_for_route(driver_id):
    """Get detailed driver information for route tracking"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, name, rank, army_number, unit
            FROM drivers 
            WHERE id = %s
        """, (driver_id,))

        driver = cursor.fetchone()

        if driver:
            return jsonify({
                'success': True,
                'driver': {
                    'id': driver[0],
                    'name': driver[1],
                    'rank': driver[2],
                    'army_number': driver[3],
                    'unit': driver[4]
                }
            })
        else:
            return jsonify({'success': False, 'message': 'Driver not found'})

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch driver details'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/route-statistics-for-route')
def get_route_statistics_for_route():
    """Calculate and return route statistics"""
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401

    vehicle_id = request.args.get('vehicle_id')
    start_time = request.args.get('start')
    end_time = request.args.get('end')

    if not all([vehicle_id, start_time, end_time]):
        return jsonify({'success': False, 'message': 'Missing required parameters'})

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Parse datetime strings to proper format for SQL Server
        try:
            start_dt = datetime.fromisoformat(start_time.replace('T', ' '))
            end_dt = datetime.fromisoformat(end_time.replace('T', ' '))
        except ValueError as e:
            return jsonify({'success': False, 'message': 'Invalid date format'})

        # Get GPS data for calculations
        cursor.execute("""
            SELECT lat, lon, speed, timestamp, driver_id
            FROM GPSData 
            WHERE vehicle_id = %s AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp
        """, (vehicle_id, start_dt, end_dt))

        gps_points = cursor.fetchall()

        if not gps_points:
            return jsonify({'success': False, 'message': 'No GPS data found'})

        # Calculate distance
        total_distance = 0.0
        for i in range(1, len(gps_points)):
            prev_point = gps_points[i - 1]
            curr_point = gps_points[i]
            if prev_point[0] and prev_point[1] and curr_point[0] and curr_point[1]:
                distance = geodesic((prev_point[0], prev_point[1]), (curr_point[0], curr_point[1])).kilometers
                total_distance += distance

        # Calculate speeds
        speeds = [float(point[2]) if point[2] else 0 for point in gps_points]
        max_speed = max(speeds) if speeds else 0
        avg_speed = sum(speeds) / len(speeds) if speeds else 0

        # Calculate duration
        start_timestamp = gps_points[0][3]
        end_timestamp = gps_points[-1][3]
        duration_seconds = (end_timestamp - start_timestamp).total_seconds()
        duration_hours = duration_seconds / 3600

        # Count unique drivers
        unique_drivers = len(set([point[4] for point in gps_points if point[4]]))

        return jsonify({
            'success': True,
            'statistics': {
                'total_distance': round(total_distance, 2),
                'max_speed': round(max_speed, 1),
                'avg_speed': round(avg_speed, 1),
                'duration_hours': round(duration_hours, 1),
                'total_points': len(gps_points),
                'unique_drivers': unique_drivers
            }
        })

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'success': False, 'message': 'Failed to calculate statistics'})

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

################################# maintenance page ##################################

@app.route('/get_maintenance_stats')
def get_maintenance_stats():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        current_date = datetime.now().date()
        week_later = current_date + timedelta(days=7)
        month_later = current_date + timedelta(days=30)

        # Get overdue maintenance
        cursor.execute("""
            SELECT COUNT(*) FROM maintenance_records mr
            INNER JOIN vehicles v ON mr.vehicle_id = v.id
            WHERE mr.status = 'not' AND (
                (mr.next_due IS NOT NULL AND mr.next_due < %s) OR
                (mr.next_due_mileage IS NOT NULL AND mr.next_due_mileage <= v.total_milage)
            )
        """, (current_date,))
        overdue = cursor.fetchone()[0]

        # Get due this week
        cursor.execute("""
            SELECT COUNT(*) FROM maintenance_records mr
            INNER JOIN vehicles v ON mr.vehicle_id = v.id
            WHERE mr.status = 'not' AND (
                (mr.next_due IS NOT NULL AND mr.next_due BETWEEN %s AND %s) OR
                (mr.next_due_mileage IS NOT NULL AND mr.next_due_mileage BETWEEN v.total_milage AND v.total_milage + 500)
            )
        """, (current_date, week_later))
        this_week = cursor.fetchone()[0]

        # Get due this month
        cursor.execute("""
            SELECT COUNT(*) FROM maintenance_records mr
            INNER JOIN vehicles v ON mr.vehicle_id = v.id
            WHERE mr.status = 'not' AND (
                (mr.next_due IS NOT NULL AND mr.next_due BETWEEN %s AND %s) OR
                (mr.next_due_mileage IS NOT NULL AND mr.next_due_mileage BETWEEN v.total_milage + 500 AND v.total_milage + 2000)
            )
        """, (week_later, month_later))
        this_month = cursor.fetchone()[0]

        # Get on track
        cursor.execute("""
            SELECT COUNT(*) FROM maintenance_records mr
            INNER JOIN vehicles v ON mr.vehicle_id = v.id
            WHERE mr.status = 'not' AND (
                (mr.next_due IS NOT NULL AND mr.next_due > %s) OR
                (mr.next_due_mileage IS NOT NULL AND mr.next_due_mileage > v.total_milage + 2000)
            )
        """, (month_later,))
        on_track = cursor.fetchone()[0]

        return jsonify({
            'overdue': overdue,
            'this_week': this_week,
            'this_month': this_month,
            'on_track': on_track
        })

    except Exception as e:
        print(f"Error getting maintenance stats: {e}")
        return jsonify({'error': 'Database error'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/get_maintenance_records')
def get_maintenance_records():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    page = int(request.args.get('page', 1))
    per_page = 20
    offset = (page - 1) * per_page

    maintenance_type = request.args.get('maintenance_type', '')
    status_filter = request.args.get('status', '')
    vehicle_make = request.args.get('vehicle_make', '')
    vehicle_type = request.args.get('vehicle_type', '')

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        current_date = datetime.now().date()
        week_later = current_date + timedelta(days=7)
        month_later = current_date + timedelta(days=30)

        # Build WHERE clause
        where_conditions = ["mr.status = 'not'"]
        where_params = []

        if maintenance_type:
            where_conditions.append("mr.type = %s")
            where_params.append(maintenance_type)

        if vehicle_make:
            where_conditions.append("v.make = %s")
            where_params.append(vehicle_make)

        if vehicle_type:
            where_conditions.append("v.type = %s")
            where_params.append(vehicle_type)

        where_clause = " AND ".join(where_conditions)

        # Get total count
        count_query = f"""
            SELECT COUNT(*) FROM maintenance_records mr
            INNER JOIN vehicles v ON mr.vehicle_id = v.id
            WHERE {where_clause}
        """
        cursor.execute(count_query, where_params)
        total_records = cursor.fetchone()[0]
        total_pages = (total_records + per_page - 1) // per_page

        # Get records with status calculation - separate queries to avoid parameter conflicts
        base_query = f"""
            SELECT mr.vehicle_id, v.BA_number, v.make, v.type, mr.type as maintenance_type,
                   mr.last_done, mr.next_due, mr.next_due_mileage, mr.periodicity,
                   v.total_milage
            FROM maintenance_records mr
            INNER JOIN vehicles v ON mr.vehicle_id = v.id
            WHERE {where_clause}
            ORDER BY mr.next_due, mr.next_due_mileage
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """

        # Execute with proper parameters
        query_params = where_params + [offset, per_page]
        cursor.execute(base_query, query_params)

        records = []
        for row in cursor.fetchall():
            # Calculate status in Python to avoid SQL parameter issues
            status_category = 'on_track'
            status_text = 'On Track'

            next_due = row[6]  # mr.next_due
            next_due_mileage = row[7]  # mr.next_due_mileage
            current_mileage = row[9] or 0  # v.total_milage

            # Check if overdue
            if ((next_due and next_due < current_date) or
                    (next_due_mileage and next_due_mileage <= current_mileage)):
                status_category = 'overdue'
                status_text = 'Overdue'
            # Check if due soon (within a week)
            elif ((next_due and current_date <= next_due <= week_later) or
                  (next_due_mileage and current_mileage <= next_due_mileage <= current_mileage + 500)):
                status_category = 'due_soon'
                status_text = 'Due Soon'
            # Check if scheduled (within a month)
            elif ((next_due and week_later < next_due <= month_later) or
                  (next_due_mileage and current_mileage + 500 < next_due_mileage <= current_mileage + 2000)):
                status_category = 'scheduled'
                status_text = 'Scheduled'

            # Apply status filter if specified
            if status_filter and status_category != status_filter:
                continue

            records.append({
                'vehicle_id': row[0],
                'ba_number': row[1],
                'make': row[2],
                'vehicle_type': row[3],
                'maintenance_type': row[4],
                'last_done': row[5].strftime('%Y-%m-%d') if row[5] else None,
                'next_due': row[6].strftime('%Y-%m-%d') if row[6] else None,
                'status': status_category,
                'status_text': status_text,
                'periodicity': row[8]
            })

        return jsonify({
            'records': records,
            'total_records': total_records,
            'total_pages': total_pages,
            'current_page': page
        })

    except Exception as e:
        print(f"Error getting maintenance records: {e}")
        return jsonify({'error': 'Database error'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/get_vehicles_list')
def get_vehicles_list():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, BA_number, make, model FROM vehicles ORDER BY BA_number")
        vehicles = []
        for row in cursor.fetchall():
            vehicles.append({
                'id': row[0],
                'ba_number': row[1],
                'make': row[2],
                'model': row[3]
            })

        return jsonify({'vehicles': vehicles})

    except Exception as e:
        print(f"Error getting vehicles list: {e}")
        return jsonify({'error': 'Database error'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/get_maintenance_filters')
def get_maintenance_filters():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get unique makes
        cursor.execute("SELECT DISTINCT make FROM vehicles ORDER BY make")
        makes = [row[0] for row in cursor.fetchall()]

        # Get unique vehicle types
        cursor.execute("SELECT DISTINCT type FROM vehicles ORDER BY type")
        vehicle_types = [row[0] for row in cursor.fetchall()]

        return jsonify({
            'makes': makes,
            'vehicle_types': vehicle_types
        })

    except Exception as e:
        print(f"Error getting filter options: {e}")
        return jsonify({'error': 'Database error'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/get_vehicle_groups')
def get_vehicle_groups():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT make, type, model, COUNT(*) as count
            FROM vehicles
            GROUP BY make, type, model
            ORDER BY make, type, model
        """)

        groups = []
        for row in cursor.fetchall():
            groups.append({
                'make': row[0],
                'type': row[1],
                'model': row[2],
                'count': row[3]
            })

        return jsonify({'groups': groups})

    except Exception as e:
        print(f"Error getting vehicle groups: {e}")
        return jsonify({'error': 'Database error'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/get_vehicle_by_id_for_maintenance')
def get_vehicle_by_id_for_maintenance():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    vehicle_id = request.args.get('vehicle_id')
    if not vehicle_id:
        return jsonify({'error': 'Vehicle ID required'}), 400

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, BA_number, make, type, model, total_milage, unit, maintaining_workshop
            FROM vehicles WHERE id = %s
        """, (vehicle_id,))

        row = cursor.fetchone()
        if row:
            vehicle = {
                'id': row[0],
                'ba_number': row[1],
                'make': row[2],
                'type': row[3],
                'model': row[4],
                'total_milage': row[5],
                'unit': row[6],
                'maintaining_workshop': row[7]
            }
            return jsonify({'vehicle': vehicle})
        else:
            return jsonify({'error': 'Vehicle not found'}), 404

    except Exception as e:
        print(f"Error getting vehicle by ID: {e}")
        return jsonify({'error': 'Database error'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/get_maintenance_criteria')
def get_maintenance_criteria():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    vehicle_id = request.args.get('vehicle_id')
    maintenance_type = request.args.get('maintenance_type')

    if not vehicle_id or not maintenance_type:
        return jsonify({'error': 'Vehicle ID and maintenance type required'}), 400

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT periodicity, time_interval_months, distance_interval_km
            FROM maintenance_records 
            WHERE vehicle_id = %s AND type = %s AND status = 'not'
        """, (vehicle_id, maintenance_type))

        row = cursor.fetchone()
        if row:
            criteria = {
                'periodicity': row[0],
                'time_interval_months': row[1],
                'distance_interval_km': row[2]
            }
            return jsonify({'success': True, 'criteria': criteria})
        else:
            return jsonify({'success': False, 'message': 'No maintenance record found'}), 404

    except Exception as e:
        print(f"Error getting maintenance criteria: {e}")
        return jsonify({'error': 'Database error'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/get_group_maintenance_criteria')
def get_group_maintenance_criteria():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    make = request.args.get('make')
    vehicle_type = request.args.get('type')
    model = request.args.get('model')
    maintenance_type = request.args.get('maintenance_type')

    if not all([make, vehicle_type, model, maintenance_type]):
        return jsonify({'error': 'All parameters required'}), 400

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT TOP 1 mr.periodicity, mr.time_interval_months, mr.distance_interval_km
            FROM maintenance_records mr
            INNER JOIN vehicles v ON mr.vehicle_id = v.id
            WHERE v.make = %s AND v.type = %s AND v.model = %s 
            AND mr.type = %s AND mr.status = 'not'
        """, (make, vehicle_type, model, maintenance_type))

        row = cursor.fetchone()
        if row:
            criteria = {
                'periodicity': row[0],
                'time_interval_months': row[1],
                'distance_interval_km': row[2]
            }
            return jsonify({'success': True, 'criteria': criteria})
        else:
            return jsonify({'success': False, 'message': 'No maintenance record found for this group'}), 404

    except Exception as e:
        print(f"Error getting group maintenance criteria: {e}")
        return jsonify({'error': 'Database error'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/update_maintenance_record', methods=['POST'])
def update_maintenance_record():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.get_json()
    vehicle_id = data.get('vehicle_id')
    maintenance_type = data.get('maintenance_type')
    completion_date = data.get('completion_date')
    completion_mileage = data.get('completion_mileage')

    if not all([vehicle_id, maintenance_type, completion_date, completion_mileage]):
        return jsonify({'success': False, 'message': 'All fields are required'}), 400

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Find the existing 'not' record
        cursor.execute("""
            SELECT id, periodicity, time_interval_months, distance_interval_km
            FROM maintenance_records 
            WHERE vehicle_id = %s AND type = %s AND status = 'not'
        """, (vehicle_id, maintenance_type))

        existing_record = cursor.fetchone()
        if not existing_record:
            return jsonify({'success': False, 'message': 'No pending maintenance record found'}), 404

        record_id, periodicity, time_interval_months, distance_interval_km = existing_record

        # Update the existing record to 'done'
        cursor.execute("""
            UPDATE maintenance_records 
            SET status = 'done', completion_date = %s, completion_mileage = %s
            WHERE id = %s
        """, (completion_date, completion_mileage, record_id))

        # Calculate next due date and mileage
        completion_date_obj = datetime.strptime(completion_date, '%Y-%m-%d').date()
        next_due = None
        next_due_mileage = None

        if periodicity in [0, 2] and time_interval_months:  # Time-based or both
            next_due = completion_date_obj + timedelta(days=30 * time_interval_months)

        if periodicity in [1, 2] and distance_interval_km:  # Distance-based or both
            next_due_mileage = completion_mileage + distance_interval_km

        # Create new 'not' record
        cursor.execute("""
            INSERT INTO maintenance_records 
            (vehicle_id, type, last_done, last_done_mileage, next_due, next_due_mileage, 
             periodicity, time_interval_months, distance_interval_km, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'not')
        """, (vehicle_id, maintenance_type, completion_date, completion_mileage,
              next_due, next_due_mileage, periodicity, time_interval_months, distance_interval_km))

        conn.commit()
        return jsonify({'success': True, 'message': 'Maintenance record updated successfully'})

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error updating maintenance record: {e}")
        return jsonify({'success': False, 'message': 'Database error'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/export_previous_records', methods=['POST'])
def export_previous_records():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.get_json()
    vehicle_id = data.get('vehicle_id')
    maintenance_types = data.get('maintenance_types', [])

    if not vehicle_id or not maintenance_types:
        return jsonify({'error': 'Vehicle ID and maintenance types required'}), 400

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get vehicle details
        cursor.execute("""
            SELECT BA_number, make, type, model, total_milage, unit, maintaining_workshop
            FROM vehicles WHERE id = %s
        """, (vehicle_id,))

        vehicle = cursor.fetchone()
        if not vehicle:
            return jsonify({'error': 'Vehicle not found'}), 404

        # Get maintenance records
        placeholders = ','.join(['%s' for _ in maintenance_types])
        query = f"""
            SELECT type, last_done, last_done_mileage, completion_date, completion_mileage,
                   periodicity, time_interval_months, distance_interval_km
            FROM maintenance_records 
            WHERE vehicle_id = %s AND type IN ({placeholders}) AND status = 'done'
            ORDER BY completion_date DESC
        """

        cursor.execute(query, [vehicle_id] + maintenance_types)
        records = cursor.fetchall()

        # Create CSV
        output = StringIO()
        writer = csv.writer(output)

        # Write headers
        writer.writerow([
            'Vehicle ID', 'BA Number', 'Make', 'Type', 'Model', 'Total Mileage', 'Unit', 'Workshop',
            'Maintenance Type', 'Last Done Date', 'Last Done Mileage', 'Completion Date',
            'Completion Mileage', 'Periodicity', 'Time Interval (Months)', 'Distance Interval (KM)'
        ])

        # Write data
        for record in records:
            periodicity_text = {0: 'Time Based', 1: 'Distance Based', 2: 'Both'}.get(record[5], 'Unknown')
            writer.writerow([
                vehicle_id, vehicle[0], vehicle[1], vehicle[2], vehicle[3], vehicle[4],
                vehicle[5], vehicle[6], record[0], record[1], record[2], record[3],
                record[4], periodicity_text, record[6], record[7]
            ])

        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=maintenance_previous_records_{vehicle[0]}.csv'}
        )

    except Exception as e:
        print(f"Error exporting previous records: {e}")
        return jsonify({'error': 'Export failed'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/export_current_status', methods=['POST'])
def export_current_status():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.get_json()
    vehicle_id = data.get('vehicle_id')
    maintenance_types = data.get('maintenance_types', [])

    if not vehicle_id or not maintenance_types:
        return jsonify({'error': 'Vehicle ID and maintenance types required'}), 400

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get vehicle details
        cursor.execute("""
            SELECT BA_number, make, type, model, total_milage, unit, maintaining_workshop
            FROM vehicles WHERE id = %s
        """, (vehicle_id,))

        vehicle = cursor.fetchone()
        if not vehicle:
            return jsonify({'error': 'Vehicle not found'}), 404

        # Get maintenance records
        placeholders = ','.join(['%s' for _ in maintenance_types])
        query = f"""
            SELECT type, last_done, last_done_mileage, next_due, next_due_mileage,
                   periodicity, time_interval_months, distance_interval_km
            FROM maintenance_records 
            WHERE vehicle_id = %s AND type IN ({placeholders}) AND status = 'not'
            ORDER BY next_due, next_due_mileage
        """

        cursor.execute(query, [vehicle_id] + maintenance_types)
        records = cursor.fetchall()

        # Create CSV
        output = StringIO()
        writer = csv.writer(output)

        # Write headers
        writer.writerow([
            'Vehicle ID', 'BA Number', 'Make', 'Type', 'Model', 'Total Mileage', 'Unit', 'Workshop',
            'Maintenance Type', 'Last Done Date', 'Last Done Mileage', 'Next Due Date',
            'Next Due Mileage', 'Periodicity', 'Time Interval (Months)', 'Distance Interval (KM)'
        ])

        # Write data
        for record in records:
            periodicity_text = {0: 'Time Based', 1: 'Distance Based', 2: 'Both'}.get(record[5], 'Unknown')
            writer.writerow([
                vehicle_id, vehicle[0], vehicle[1], vehicle[2], vehicle[3], vehicle[4],
                vehicle[5], vehicle[6], record[0], record[1], record[2], record[3],
                record[4], periodicity_text, record[6], record[7]
            ])

        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=maintenance_current_status_{vehicle[0]}.csv'}
        )

    except Exception as e:
        print(f"Error exporting current status: {e}")
        return jsonify({'error': 'Export failed'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/update_single_maintenance_criteria', methods=['POST'])
def update_single_maintenance_criteria():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.get_json()
    vehicle_id = data.get('vehicle_id')
    maintenance_type = data.get('maintenance_type')
    time_interval_months = data.get('time_interval_months')
    distance_interval_km = data.get('distance_interval_km')

    if not vehicle_id or not maintenance_type:
        return jsonify({'success': False, 'message': 'Vehicle ID and maintenance type required'}), 400

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Update the maintenance record
        update_fields = []
        params = []

        if time_interval_months is not None:
            update_fields.append("time_interval_months = %s")
            params.append(time_interval_months)

        if distance_interval_km is not None:
            update_fields.append("distance_interval_km = %s")
            params.append(distance_interval_km)

        if not update_fields:
            return jsonify({'success': False, 'message': 'No criteria provided'}), 400

        params.extend([vehicle_id, maintenance_type])

        query = f"""
            UPDATE maintenance_records 
            SET {', '.join(update_fields)}
            WHERE vehicle_id = %s AND type = %s AND status = 'not'
        """

        cursor.execute(query, params)

        if cursor.rowcount == 0:
            return jsonify({'success': False, 'message': 'No maintenance record found to update'}), 404

        conn.commit()
        return jsonify({'success': True, 'message': 'Maintenance criteria updated successfully'})

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error updating single maintenance criteria: {e}")
        return jsonify({'success': False, 'message': 'Database error'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/update_group_maintenance_criteria', methods=['POST'])
def update_group_maintenance_criteria():
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.get_json()
    make = data.get('make')
    vehicle_type = data.get('type')
    model = data.get('model')
    maintenance_type = data.get('maintenance_type')
    time_interval_months = data.get('time_interval_months')
    distance_interval_km = data.get('distance_interval_km')

    if not all([make, vehicle_type, model, maintenance_type]):
        return jsonify({'success': False, 'message': 'All vehicle group parameters and maintenance type required'}), 400

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Update maintenance records for all vehicles in the group
        update_fields = []
        params = []

        if time_interval_months is not None:
            update_fields.append("time_interval_months = %s")
            params.append(time_interval_months)

        if distance_interval_km is not None:
            update_fields.append("distance_interval_km = %s")
            params.append(distance_interval_km)

        if not update_fields:
            return jsonify({'success': False, 'message': 'No criteria provided'}), 400

        params.extend([make, vehicle_type, model, maintenance_type])

        query = f"""
            UPDATE maintenance_records 
            SET {', '.join(update_fields)}
            WHERE vehicle_id IN (
                SELECT id FROM vehicles 
                WHERE make = %s AND type = %s AND model = %s
            ) AND type = %s AND status = 'not'
        """

        cursor.execute(query, params)

        if cursor.rowcount == 0:
            return jsonify({'success': False, 'message': 'No maintenance records found to update for this group'}), 404

        conn.commit()
        return jsonify({'success': True, 'message': f'Maintenance criteria updated for {cursor.rowcount} vehicles'})

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error updating group maintenance criteria: {e}")
        return jsonify({'success': False, 'message': 'Database error'}), 500
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    app.run(debug=True)
