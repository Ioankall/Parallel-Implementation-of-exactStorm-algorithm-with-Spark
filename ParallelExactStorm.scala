import { Injectable, Injector } from '@angular/core';
import { Router } from '@angular/router';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import 'rxjs/add/observable/forkJoin';
import 'rxjs/add/operator/merge';
import 'rxjs/add/observable/fromPromise';
import { Response } from '@angular/http';
import { Account } from '../domain/account.model';
import { classToPlain, plainToClass } from '@gkaran/class-transformer';
import { Creditcard } from '../domain/creditcard.model';
import { Address } from '../domain/address.model';
import { Coupon } from '../domain/coupon.model';
import { LoyaltyNotification } from '../domain/loyalty-notification.model';
import { LoyaltyMembership } from '../domain/loyalty-membership.model';
import { HypermediaService } from './services/hypermedia.service';
import { Password } from '../domain/password.model';
import { LoyaltyHistoryRecord } from '../domain/loyalty-history-record.model';
import { LoyaltyBadge } from '../domain/loyalty-badge.model';
import { LoyaltyMembershipDetails } from '../domain/loyalty-membership-details';
import { Order } from '../domain/order.model';
import { Subject } from 'rxjs/Subject';
import { GiftcardWrapper } from '../domain/giftcard-wrapper.model';
import { Giftcard } from '../domain/giftcard.model';
import { CustomerInfo } from '../domain/customer-info.model';
import { HttpClient } from './http-client.service';
import { RequestStrategy } from '../domain/request-strategy';
import { CreditCardMaskPipe } from '../shared/credit-card-mask.pipe';
import { LoyaltySignup } from 'app/domain/loyalty-signup.model';
import { AuditService } from './audit.service';
import {FacebookService, InitParams, LoginOptions} from 'ngx-facebook';
import {StoreService} from './store.service';
import {AccountCharity} from '../domain/account-charity.model';
import {RecentlyOrderedItem} from '../domain/recently-ordered-item.model';
import {OrderPaymentTypesService} from './services/order-payment-types.service';
import {isNullOrUndefined} from 'util';

@Injectable()
export class AccountService {
  readonly BASE_PATH = '/ws/integrated/v1/ordering/account';
  readonly LOYALTY_BASE_PATH = this.BASE_PATH + '/loyalty';
  private readonly VERIFIED_ACCOUNT_STORAGE = 'ACCOUNT_VERIFIED';

  private _account: BehaviorSubject<Account> = new BehaviorSubject(Account.createDummyAccount());
  public account: Observable<Account> = this._account.asObservable();

  private _recentlyOrderedItems: BehaviorSubject<RecentlyOrderedItem[]> = new BehaviorSubject([]);
  public recentlyOrderedItems: Observable<RecentlyOrderedItem[]> = this._recentlyOrderedItems.asObservable();

  private reloadAddresses: Subject<void> = new Subject<void>();
  public reloadCoupons: Subject<void> = new Subject<void>();
  public reloadCreditCards: Subject<void> = new Subject<void>();
  private ccMaskedPipe = new CreditCardMaskPipe();
  private router: Router;

  private initializedListener: boolean = false;

  constructor(private http: HttpClient,
              private hypermedia: HypermediaService,
              private auditService: AuditService,
              private fb: FacebookService,
              private storeService: StoreService,
              private orderPaymentTypesService: OrderPaymentTypesService,
              private injector: Injector) {
    this.router = this.injector.get(Router);
    this.storeService.storeConfig.subscribe(storeConfig => {
      const initParams: InitParams = {
        appId: storeConfig.facebookAppId,
        xfbml: true,
        version: 'v2.8'
      };

      if (!isNullOrUndefined(fb)) {
        fb.init(initParams);
      }
    });
    this.initVerifiedAccountListener();
  }

  login(email: string, password: string): Observable<Account> {
    return this.http
      .post(this.BASE_PATH + '/login', {email, password}, {}, RequestStrategy.HATEOAS)
      .map(response => response.json() as Object)
      .map(json => plainToClass(Account, json))
      .do(account => this.auditService.createAudit(() => `User ${account.email} has logged in`))
      .do(account => this._account.next(account))
      .do(() => this.orderPaymentTypesService.loadPaymentTypes());
  }

  facebookLogin() {
    const loginOptions: LoginOptions = {
      scope: 'public_profile, email',
      auth_type: 'rerequest'
    };

    return Observable.fromPromise(this.fb.login(loginOptions)).flatMap(response => this.http.post(this.BASE_PATH + '/facebookLogin',
      {token: response.authResponse.accessToken}, {}, RequestStrategy.HATEOAS))
      .map(response => response.json() as Object)
      .map(json => plainToClass(Account, json))
      .do(account => this._account.next(account));
  }

  forgotPassword(email: string): Observable<any> {
    return this.http
      .put(this.BASE_PATH + '/remindPassword', {email})
      .do(() => this.auditService.createAudit(() => `Email has been sent to ${email} reset the password`));
  }

  resetPassword(password: string, passwordConfirmation: string, key: string): Observable<any> {
    return this.http
      .put(this.BASE_PATH + '/resetPassword', {password, passwordConfirmation, key})
      .do(() => this.auditService.createAudit(() => 'Password has been reset'));
  }

  verify(token: string): Observable<any> {
    return this.http
      .put(this.BASE_PATH + '/verify', {token})
      .do(() => this.auditService.createAudit(() => 'Email has been verified'))
      .do(() => localStorage.setItem(this.VERIFIED_ACCOUNT_STORAGE, 'true'))
      .do(() => this.fetchAccount());
  }

  unsubscribe(email: string): Observable<any> {
    return this.http
      .put(this.BASE_PATH + '/unsubscribe', {email})
      .do(() => this.auditService.createAudit(() => 'Email has been unsubscribed'));
  }

  logout(activatedRouteData: Observable<{[name: string]: any}>): void {
    this.auditService.createAudit(() => `User ${this._account.value.email} has logged out`);
    this.http
      .delete(this.BASE_PATH + '/logout')
      .flatMap(() => activatedRouteData)
      .map(data => data['skipRedirect'])
      .flatMap(skipRedirect => {
        if (skipRedirect) {
          return this.router.navigate(['/']);
        }
        return Promise.resolve({});
      })
      .subscribe(() => {
        window.location.reload(true);
      });
  }

  register(account: Account): Observable<Account> {
    return this.http
      .post(this.BASE_PATH, classToPlain(account), {}, RequestStrategy.HATEOAS)
      .map(response => response.json() as Object)
      .map(json => plainToClass(Account, json))
      .do(ac => this.auditService.createAudit(() => `Account ${ac.email} has been created`))
      .do(ac => this._account.next(ac));
  }

  signupForLoyalty(data: LoyaltySignup): Observable<void> {
    return this.http
      .post(this.LOYALTY_BASE_PATH, classToPlain(data))
      .map(() => {})
      .do(() => this.auditService.createAudit(() => (
        {message: `User with cellphone ${data.cellphone} has signed up for loyalty`})))
      .do(() => this.fetchAccount());
  }

  fetchAccount(): Promise<void> {
    return this.http
      .get(this.BASE_PATH, {}, RequestStrategy.HATEOAS)
      .map(response => response.json() as Object)
      .map(json => plainToClass(Account, json))
      .map(account => this._account.next(account))
      .toPromise()
      .catch(() => this._account.next(new Account()));
  }

  getCustomerInfoFromZZZ(zzz: string): Observable<CustomerInfo> {
    return this.http
      .get(`${this.BASE_PATH}/${zzz}`)
      .map(response => response.json() as Object)
      .map(json => plainToClass(CustomerInfo, json));
  }

  updateAccount(newAccount: Account): Observable<Account> {
    return this.http
      .put(this.BASE_PATH, classToPlain(newAccount), {}, RequestStrategy.HATEOAS)
      .map(response => response.json() as Object)
      .map(json => plainToClass(Account, json))
      .do(account => {
        const {firstName, lastName, phone, email, subscribed} = account;
        const details = {firstName, lastName, phone, email, subscribed};
        this.auditService.createAudit(() => (
            {message: `Account ${account.email} has been updated`, details}));
        }
      )
      .do(account => this._account.next(account));
  }

  updatePassword(password: Password): Observable<any> {
    return this.http
      .put(this.BASE_PATH + '/password', password)
      .do(() => this.auditService.createAudit(() => 'Password has been updated'));
  }

  deleteAccount(): Observable<void> {
    const accountEmail = this._account.getValue().email;
    this.auditService.createAudit(() => `Account ${accountEmail} has been deleted`);
    return this.http
      .delete(this.BASE_PATH)
      .map(response => { })
      .do(() => {
        window.location.reload(true);
      });
  }

  resendVerificationEmail(email: string): Observable<Response> {
    return this.http.post(this.BASE_PATH + '/resendVerificationEmail', {email: email})
      .do(() => this.auditService.createAudit(() => `Email verification has been sent to ${email}`));
  }

  /* CREDIT CARDS */
  addCreditCard(creditCard: Creditcard): Observable<Creditcard> {
    return this.http
      .post(`${this.BASE_PATH}/creditCards`, classToPlain(creditCard))
      .map(response => response.json() as Object)
      .map(json => plainToClass(Creditcard, json))
      .do(cc => {
        const maskedNumber = this.ccMaskedPipe.transform(cc.lastDigits);
        this.auditService.createAudit(() => `Credit Card "${maskedNumber}" has been added to wallet`);
      })
      .do(() => this.reloadCreditCards.next());
  }

  updateCreditCard(creditCard: Creditcard): Observable<Creditcard> {
    return this.http
      .put(`${this.BASE_PATH}/creditCards/${creditCard.ordinal}`, classToPlain(creditCard))
      .map(response => response.json() as Object)
      .map(json => plainToClass(Creditcard, json))
      .do(cc => {
        const maskedNumber = this.ccMaskedPipe.transform(cc.lastDigits);
        this.auditService.createAudit(() => `Credit Card "${maskedNumber}" has been updated`);
      })
      .do(() => this.reloadCreditCards.next());
  }

  deleteCreditCard(creditCard: Creditcard): Observable<void> {
    const ordinal = creditCard.ordinal;
    const maskedNumber = this.ccMaskedPipe.transform(creditCard.lastDigits);
    return this.http
      .delete(`${this.BASE_PATH}/creditCards/${ordinal}`)
      .map(() => { })
      .do(() => this.auditService.createAudit(() => `Credit Card "${maskedNumber}" has been deleted`))
      .do(() => this.reloadCreditCards.next());
  }

  addAddress(address: Address): Observable<Address> {
    return this.http
      .post(`${this.BASE_PATH}/addresses`, classToPlain(address))
      .map(response => response.json() as Object)
      .map(json => plainToClass(Address, json))
      .do(() => this.auditService.createAudit(() =>
        ({message: `New address has been created`, details: {addressName: address.name, fullAddress: address.fullAddress}})))
      .do(() => this.reloadAddresses.next());
  }

  updateAddress(address: Address): Observable<Address> {
    return this.http
      .put(`${this.BASE_PATH}/addresses/${address.name}`, classToPlain(address))
      .map(response => response.json() as Object)
      .map(json => plainToClass(Address, json))
      .do(() => this.auditService.createAudit(() =>
        ({message: `Address has been updated`, details: {addressName: address.name, fullAddress: address.fullAddress}})))
      .do(() => this.reloadAddresses.next());
  }

  deleteAddress(address: Address): Observable<void> {
    return this.http
      .delete(`${this.BASE_PATH}/addresses/${address.name}`)
      .map(() => { }) // map response to undefined to satisfy expectation for Observable<void>
      .do(() => this.reloadAddresses.next())
      .do(() => this.auditService.createAudit(() =>
        ({message: `Address has been deleted`, details: {addressName: address.name, fullAddress: address.fullAddress}})));
  }

  fetchLoyaltyNotifications(email: string, token: string): Observable<LoyaltyNotification[]> {
    // We use encodeURIComponent here since we might have emails with characters that should be escaped and
    // angular escapes them (they have a bug here).
    const query = `Email=${encodeURIComponent(email)}&token=${encodeURIComponent(token)}`;
    return this.http
      .get(`/ws/integrated/v1/ordering/account/loyalty/notifications?${query}`)
      .map(response => response.json())
      .map(json => plainToClass(LoyaltyNotification, json));
  }

  updateLoyaltyNotifications(membership: LoyaltyMembership, notifications: LoyaltyNotification[]) {
    const notificationsLink = this.hypermedia.findLink(membership.links, 'notifications');
    if (!notificationsLink) {
      const noLinkMessage = 'No notifications link has been found';
      this.auditService.createAudit(() => noLinkMessage);
      throw new Error(noLinkMessage);
    }
    return this.http.put(notificationsLink.href, notifications)
      .map(response => response.json())
      .map(json => plainToClass(LoyaltyNotification, json));
  }

  updateLoyaltyNotificationsViaToken(email: string, token: string,
                                     notifications: LoyaltyNotification[]): Observable<LoyaltyNotification[]> {
    return this.http
      .post('/ws/integrated/v1/ordering/account/loyalty/notifications', {
        email,
        token,
        notifications: classToPlain(notifications)
      })
      .map(response => response.json())
      .map(json => json.notifications)
      .map(n => plainToClass(LoyaltyNotification, n))
      .do(loyaltyNotification => this.auditService.createAudit(() => (
        {message: 'Loyalty notifications have been updated', details: { notifications }})));
  }

  getLoyaltyMembership(): Observable<LoyaltyMembership> {
    return this.account
      .filter(account => account.isInstantiated && account.signUpForLoyalty)
      .flatMap(account => this.hypermedia.get(account.links, 'memberships'))
      .map(response => response.json())
      .map(json => plainToClass(LoyaltyMembership, json))
      .map(memberships => memberships[0]);
  }

  getLoyaltyMembershipDetails(membership: LoyaltyMembership): Observable<LoyaltyMembershipDetails> {
    return Observable
      .forkJoin([
        this.hypermedia.get(membership.links, 'history'),
        this.hypermedia.get(membership.links, 'badges'),
        this.hypermedia.get(membership.links, 'notifications')
      ])
      .map(results => results.map(r => r.json()))
      .map(results => ({
        history: plainToClass(LoyaltyHistoryRecord, results[0])
          .sort((a, b) => b.date.toDate().getTime() - a.date.toDate().getTime()),
        badges: plainToClass(LoyaltyBadge, results[1]),
        notifications: plainToClass(LoyaltyNotification, results[2])
      }) as LoyaltyMembershipDetails);
  }

  getMyOrders(): Observable<Order[]> {
    return this.account
      .filter(account => account.isInstantiated)
      .flatMap(account => this.hypermedia.get(account.links, 'orders'))
      .map(response => response.json())
      .map(json => plainToClass(Order, json));
  }

  getAddresses(): Observable<Address[]> {
    return this.account
      .merge(this.reloadAddresses.asObservable())
      .flatMap(() => this.account)
      .filter(account => account.isInstantiated)
      .flatMap(account => this.hypermedia.get(account.links, 'addresses'))
      .map(response => response.json())
      .map(json => plainToClass(Address, json));
  }

  getCoupons(): Observable<Coupon[]> {
    return this.account
      .merge(this.reloadCoupons.asObservable())
      .flatMap(() => this.account)
      .filter(account => account.isInstantiated)
      .flatMap(account => this.hypermedia.get(account.links, 'coupons'))
      .map(response => response.json())
      .map(json => plainToClass(Coupon, json))
      .map(coupons => coupons.sort(c => c && c.isApplicable ? -1 : 1));
  }

  getCharities(): Observable<AccountCharity[]> {
    return this.account
      .filter(account => account.isInstantiated)
      .flatMap(account => this.hypermedia.get(account.links, 'charities'))
      .map(response => response.json())
      .map(json => plainToClass(AccountCharity, json));
  }

  getCreditCards(): Observable<Creditcard[]> {
    return this.account
      .merge(this.reloadCreditCards.asObservable())
      .flatMap(() => this.account)
      .flatMap(() => this.storeService.storeConfig, (account, storeConfig) => ({ account, storeConfig }))
      .filter(({ account, storeConfig }) => account.isInstantiated && storeConfig.hasCreditCard)
      .flatMap(({ account }) => this.hypermedia.get(account.links, 'creditCards'))
      .map(response => response.json())
      .map(json => plainToClass(Creditcard, json));
  }

  getGiftCards(): Observable<Giftcard[]> {
    return this.account
      .flatMap(() => this.account)
      .filter(account => account.isInstantiated)
      .flatMap(account => this.hypermedia.get(account.links, 'giftCards'))
      .map(response => response.json() as Object)
      .map(json => plainToClass(GiftcardWrapper, json))
      .map(wrapper => wrapper.data);
  }

  getRecentlyOrderedItems(): Observable<RecentlyOrderedItem[]> {
    return this.account
      .flatMap(() => this.account)
      .flatMap(account => {
        if (account.isInstantiated) {
          return this.http.get(`${this.BASE_PATH}/mostOrderedItems`)
            .map(response => response.json())
            .map(json => plainToClass(RecentlyOrderedItem, json))
            .catch(() => Observable.of([]));
        } else {
          return Observable.of([]);
        }
      })
      .do(items => this._recentlyOrderedItems.next(items));
  }

  initVerifiedAccountListener() {
    if (!this.initializedListener && window.addEventListener) {
      console.log('init listener');
      window.addEventListener('storage', this.storageEventListener.bind(this));
    }
    this.initializedListener = true;
  }

  private storageEventListener(event: StorageEvent) {
    console.log('new event');
    if (event.storageArea === localStorage) {
      console.log('new local storage event');
      if (event.key === this.VERIFIED_ACCOUNT_STORAGE && event.newValue === 'true') {
        console.log('Fetching account');
        this.fetchAccount();
      }
    }
  }
}
